# go-data-quality-monitor-go

可嵌入的 Go 数据质量校验与增量监控引擎。以本地内存数据源模拟多批次数据输入，
在规格变化、重复/乱序批次与并发提交下保证校验结论一致且可解释。

核心包：`dq`(import `github.com/h3yow/go-data-quality-monitor-go/dq`)。

## 快速开始

```go
contract := dq.Contract{
    Version: 1,
    Fields: []dq.FieldSpec{
        {Name: "id", Type: dq.TypeInt, Required: true},
        {Name: "amount", Type: dq.TypeFloat, Required: true},
    },
    KeyGroup: []string{"id"},
}
e := dq.NewEngine(contract,
    dq.WithCompatMode(dq.CompatWarn),
    dq.WithValidatorTimeout(time.Second),
)
res, err := e.Submit(ctx, dq.Batch{ID: "b1", Seq: 1, Records: recs, EventTime: time.Now()})
metrics := e.Metrics()
```

## 本地多批次回放(Replay)

`Replayer` 在本地把预先准备好的一批数据当成多批次数据流顺序重放，**不依赖数据库、
消息队列等任何外部服务**。它走的就是 `Engine.Submit` / `Engine.EvolveDetailed` 的
同一执行路径，因此回放结论与手动逐批提交完全一致(有测试逐项比对保证)。

```go
steps := []dq.Step{
    {Kind: dq.StepSubmit, Label: "首批", Batch: dq.Batch{ID: "b1", Seq: 1, Records: recs1}},
    {Kind: dq.StepSubmit, Label: "乱序先到 seq=3", Batch: dq.Batch{ID: "b3", Seq: 3, Records: recs3}},
    {Kind: dq.StepEvolve, Label: "放宽到 v2", Contract: c2},
    {Kind: dq.StepSubmit, Label: "迟到的 seq=2", Batch: dq.Batch{ID: "b2", Seq: 2, Records: recs2}},
    {Kind: dq.StepSubmit, Label: "重复投递 b1", Batch: dq.Batch{ID: "b1", Seq: 1, Records: recs1}},
}
r := dq.NewReplayer(initial, dq.WithCompatMode(dq.CompatReject), dq.WithClock(fixedNow))
report := r.Run(ctx, steps)   // 也可逐步调用 r.SubmitStep / r.EvolveStep
for _, s := range report.Steps { /* 每批的批次标识、规则版本、接受/违规、判定原因 */ }
report.Metrics                // 最终指标快照
```

场景构造方式：

- **正常顺序**：按 Seq 递增放 `StepSubmit`；
- **乱序**：让 Seq 不按递增顺序出现(缺口存在时水位停住，补齐后一次性追上)；
- **重复投递**：重复放同一 `Batch.ID` 的提交步(重新校验、结论一致，指标只计一次)；
- **迟到批次**：在更大 Seq 之后再放较小 Seq 的批次；
- **演进交错**：把 `StepEvolve` 穿插在提交步之间。被拒/旁路的演进不改变后续批次
  使用的规则版本，`applied` 之后的批次才按新版本判。

`StepResult` 同时带批次标识、Seq、校验所用契约版本、逐条违规原因，以及演进步的
`EvolutionReport`；`ReplayReport` 附最终指标、引擎告警与契约版本。注入固定时钟
(`WithClock`)后，同一脚本重复跑得到逐字节一致的结论。

想直接观察效果而不写测试，可运行内置演示：

```bash
go run ./cmd/replaydemo
```

它会打印：每步的批次标识/规则版本/关键指标/判定原因、收紧与放宽演进的逐项解释、
最终水位与指标，并自动校验"脚本跑两遍一致"以及"回放 = 手动逐批提交"。

## 数据契约(Contract)

契约可版本化(`Version` 单调递增)，包含：

- `Fields`:字段名、类型(`string/int/float/bool`)、是否必需、约束(数值上下界 `Min/Max`、字符串长度 `MaxLen`)；
- `KeyGroup`:组成唯一键的字段组，用于批内判重。

校验(`Validator.Validate`)对每条记录产出确定结论，不合法记录**不会静默跳过**，
而是归入可区分原因(`Violation.Reason`):

| Reason | 含义 |
|---|---|
| `field_missing` | 必需字段缺失或为 null |
| `type_conflict` | 字段值类型与契约不符 |
| `constraint_breach` | 约束越界(上下界/长度) |
| `duplicate_key` | 键组与批内前序记录重复(首条接受) |
| `custom_check` | 自定义校验逻辑判定不合法 |
| `validator_fault` | 自定义校验 panic 或超时(故障被隔离) |

**可复现性**:同一契约版本对同一输入的校验结论逐字节一致——校验按契约字段
声明顺序遍历、按记录顺序产出违规，不依赖 map 迭代顺序。

## 兼容与水位语义

### 契约演进(Evolve)

`Engine.Evolve`(只返回 error)与 `Engine.EvolveDetailed`(返回可解释报告 `EvolutionReport`)
都要求版本严格递增。`AnalyzeEvolution(old, next)` 逐项产出 `Change`：每一项变化的
类别、字段、是否兼容与原因(`Change.String()` 可读)，顺序确定(按新契约字段顺序 →
被删字段 → 键组变化)。

判定原则：**只有"扩大合法数据集合"的变化才算兼容**；任何可能让原本合法数据突然
过不了的"收紧"都算不兼容。

| 变化 | 类别 | 结论 |
|---|---|---|
| 新增**可选**字段 | `field_added` | 兼容 |
| 新增**必需**字段 | `field_added` | 不兼容(收紧) |
| 删除字段 / 字段类型变化 / 键组变化 | `field_removed` / `type_changed` / `key_group_changed` | 不兼容 |
| 必填 → 可选 | `required_relaxed` | **兼容(放宽)** |
| 可选 → 必填 | `required_tightened` | **不兼容(收紧)** |
| `Min` 提高、`Max` 降低、`MaxLen` 缩短 | `constraint_tightened` | **不兼容(收窄)** |
| 原本无界、新引入上下界/长度上限 | `constraint_tightened` | **不兼容(收窄)** |
| `Min` 降低、`Max` 提高、`MaxLen` 放宽 | `constraint_relaxed` | **兼容(放宽)** |
| 解除既有上下界/长度上限 | `constraint_relaxed` | **兼容(放宽)** |

同一字段同时含放宽与收窄(例如下界放宽、上界收窄)时，变化逐项列出，只要含收紧项
整体即判不兼容。`EvolutionReport` 给出最终处置(`applied` / `rejected` / `bypassed`)、
逐项结论、产生的告警与错误；`Report.Compatible()` / `Report.Incompatible()` 便于程序化判断。

不兼容时按 `CompatMode` 策略处理：

| 策略 | 行为 |
|---|---|
| `CompatReject`(默认) | 拒绝演进，返回错误，契约保持不变 |
| `CompatBypass` | 旁路本次演进(契约不变)，记录告警 |
| `CompatWarn` | 强制应用新版本，记录告警 |

被拒绝或旁路的演进不影响后续批次：之后提交的批次仍按当前版本判定；只有 `applied`
之后的新批次才按新版本判定。

### 水位(Watermark)

`Metrics.Watermark` 表示从 1 起**已连续确认**的最大批次序号(`Batch.Seq`)。
乱序批次会被记录，但只有序号连续时才推进水位；缺口补齐后水位一次性追上。
水位单调不减。

### 去重

以 `Batch.ID` 判重：重复提交会重新校验(结论可复现)，但监控指标只计入一次，
不会造成重复计数、重复告警或基线污染。

## 指标定义(Metrics)

| 指标 | 定义 |
|---|---|
| `Watermark` | 已连续确认的最大批次序号 |
| `SeenBatches` | 去重后已处理批次数 |
| `TotalAccepted` / `TotalViolations` | 累计接受/违规记录数(去重后) |
| `Completeness` | 各契约字段在全部输入记录中的非空率 [0,1] |
| `FreshnessLag` | 当前时间与最新批次事件时间之差；事件时间只前进不倒退 |
| `DriftBaseline` | 数值字段分布基线(批均值的 Welford 运行均值) |
| `DriftAlerts` | 漂移告警：基线样本 ≥10 后，批均值偏离基线超过 3σ 触发 |

## 并发保证

- `Engine.Submit` 可任意并发调用；每个批次在提交瞬间获取契约的**一致性快照**
  (深拷贝，演进提交时也会对新契约做防御性拷贝)，校验与监控均基于该快照，
  演进中的契约不会造成半新半旧的判定。演进与提交并发交错时：被拒绝/旁路的演进
  不改变任何批次的判定版本；演进生效后，新批次才按新版本判。
- 监控状态由内部互斥锁保护，`Metrics()` 返回深拷贝快照，可安全并发读取。
- 自定义校验逻辑(`CustomValidator`)在隔离 goroutine 中执行：**panic 被恢复**、
  **超时被截断**(默认 1s，可配)，均转换为 `validator_fault` 违规；
  不会破坏引擎状态、阻塞其他校验或向调用方泄漏底层异常。
- `Submit` 自身带 panic 兜底，任何意外故障以 error 返回。

## 验证方法

```bash
go build ./...
go run ./cmd/replaydemo      # 本地多批次回放演示(乱序/重复/迟到 + 演进交错)
go test ./... -v             # 单测日志打印批次标识、规则版本、关键指标与判定原因
go test ./... -race -count=3   # 竞态检测下稳定通过
```

测试覆盖：

- `validate_test.go`:字段缺失、类型冲突、约束越界、键组重复、结论可复现、
  自定义校验 panic/超时/拒绝的隔离；
- `monitor_test.go`:乱序批次水位、重复批次幂等、新鲜度单调、完整性统计、
  分布漂移告警及重复告警抑制；
- `engine_test.go`:并发提交一致性、并发重复提交去重、三种契约演进策略、
  演进后校验生效、畸形输入与故障后引擎可用性；
- `evolution_test.go`:**必填收紧(可选→必填)/放松(必填→可选)**、
  数值上下界与字符串长度的**收窄/放宽/引入/解除**、同字段混合变化逐项可解释、
  既有破坏性变化(删字段/改类型/新增必需/键组)不回归、分析结果确定性；
- `replay_test.go`:多批次回放(乱序/重复/迟到)、中间水位变化、
  同一脚本重复回放一致、**回放与手动逐批提交逐步比对一致**；
- `interleave_test.go`:提交与收紧演进并发交错(收紧始终被拒、所有批次归 v2、
  指标不乱)、Warn 强制生效后批次归 v3 且新必填字段开始拦截、
  Bypass 脚本化交错及重复回放一致。
