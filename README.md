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

`Engine.Evolve` 要求版本严格递增。兼容性规则：

- **兼容**:新增可选字段；
- **不兼容**:删除字段、字段类型变化、新增必需字段、键组变化。

不兼容时按 `CompatMode` 策略处理：

| 策略 | 行为 |
|---|---|
| `CompatReject`(默认) | 拒绝演进，返回错误，契约保持不变 |
| `CompatBypass` | 旁路本次演进(契约不变)，记录告警 |
| `CompatWarn` | 强制应用新版本，记录告警 |

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

- `Engine.Submit` 可任意并发调用；每个批次在提交瞬间获取契约的**一致性快照**，
  校验与监控均基于该快照，演进中的契约不会造成半新半旧的判定。
- 监控状态由内部互斥锁保护，`Metrics()` 返回深拷贝快照，可安全并发读取。
- 自定义校验逻辑(`CustomValidator`)在隔离 goroutine 中执行：**panic 被恢复**、
  **超时被截断**(默认 1s，可配)，均转换为 `validator_fault` 违规；
  不会破坏引擎状态、阻塞其他校验或向调用方泄漏底层异常。
- `Submit` 自身带 panic 兜底，任何意外故障以 error 返回。

## 验证方法

```bash
go build ./...
go test ./... -v          # 单测日志打印批次标识、规则版本、关键指标与判定原因
go test ./... -race -count=3   # 竞态检测下稳定通过
```

测试覆盖：

- `validate_test.go`:字段缺失、类型冲突、约束越界、键组重复、结论可复现、
  自定义校验 panic/超时/拒绝的隔离；
- `monitor_test.go`:乱序批次水位、重复批次幂等、新鲜度单调、完整性统计、
  分布漂移告警及重复告警抑制；
- `engine_test.go`:并发提交一致性、并发重复提交去重、三种契约演进策略、
  演进后校验生效、畸形输入与故障后引擎可用性。
