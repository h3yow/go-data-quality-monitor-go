# 可嵌入数据质量校验与增量监控引擎

一个零三方依赖（仅 Go 标准库）的嵌入式引擎，用本地内存数据源模拟多批次数据输入，
在**规格变化、重复/乱序批次、并发提交**下给出一致且可解释的数据质量结论。

- 按**可版本化数据契约**校验批次：字段缺失、类型冲突、约束越界、键组重复，
  以及可插拔自定义规则（panic/超时隔离）。非法记录不静默跳过，归入互斥可区分的原因码。
- **跨批次增量监控**：事件时间水位、完整性、新鲜度、数值分布漂移基线；
  重复批次幂等，乱序批次不重复计数、不使基线倒退。
- **契约演进**按显式策略区分拒绝（reject）、旁路（sideways）与告警（alert）。
- **并发安全**：提交具备快照语义；自定义逻辑故障不破坏引擎、不阻塞其他批次、不向调用方泄漏。

## 目录结构

| 包 | 职责 |
|---|---|
| `internal/model` | 核心类型：契约、批次、原因码、结论、告警、快照、分布基线 |
| `internal/contract` | 语义化版本、契约注册表、版本间字段级 Diff、兼容策略 |
| `internal/validate` | 批次逐条校验：缺失/类型/null/约束/键组/自定义规则 |
| `internal/monitor` | 水位、完整性窗口、新鲜度、Welford 分布基线与漂移检测 |
| `internal/engine` | 并发编排：去重、版本决策、校验与监控的原子提交、快照 |
| `internal/source` | 线程安全内存数据源，支持 FIFO/逆序重放（模拟重复/乱序） |
| `cmd/demo` | 端到端可运行示例 |

## 快速开始

```go
e := engine.New(engine.Options{
    Policy:  contract.DefaultPolicy(),                       // 破坏性拒绝 / 兼容告警 / 旧版告警
    Monitor: monitor.DefaultOptions(),
})
_ = e.RegisterContract(model.Contract{ /* Name/Version/Fields/KeyFields */ })

res := e.Submit(model.Batch{
    BatchID: "b-0001", ContractV: "1.0.0",
    EventTimeMillis: 1700000000000,
    Records: []model.Record{ /* map[string]any */ },
})
fmt.Println(res.Outcome, res.ReasonCounts, res.MetricsAfter)
```

运行演示：

```bash
go run ./cmd/demo
```

## 数据契约（Data Contract）

契约是**不可变、可版本化**的规格单元：

```go
model.Contract{
    Name: "orders", Version: "1.0.0",        // 语义化版本 major.minor.patch[-pre]
    Fields: []model.FieldSpec{
        {Name: "id",    Type: model.TypeInt,    Required: true,  Nullable: false},
        {Name: "price", Type: model.TypeFloat,  Required: true,  Nullable: false,
         Min: f64(0), Max: f64(1000)},        // 数值闭区间约束
    },
    KeyFields: []string{"id"},                // 键组：组内重复即 duplicate_key_group
}
```

- 版本号按**语义化版本**比较（`1.10.0 > 1.2.0`），同名同版本重复注册报错。
- 字段类型：`string` / `int` / `float` / `bool`（整型族与整数值浮点可被接受为 int）。
- 同一版本规则对同一输入的结论**可复现**：违规按契约字段声明顺序输出，
  键组按契约键顺序拼接，不依赖 map 迭代序。

### 记录级原因码（互斥可区分，不静默跳过）

| Reason | 触发条件 |
|---|---|
| `missing_field` | 必填字段不存在 |
| `null_not_allowed` | `Nullable=false` 字段显式为 null |
| `type_conflict` | 值与声明类型不符 |
| `constraint_violation` | 数值超出 `[Min, Max]` 闭区间 |
| `duplicate_key_group` | 同一批次内键组重复（键字段本身合法即参与判定） |
| `malformed_record` | 记录为 null/非对象 |
| `custom_rule_panic` / `custom_rule_timeout` / `custom_rule_reject` | 自定义规则 panic / 超时 / 显式拒绝 |

一条记录可能携带多个违规（例如同时越界且键组重复），`ReasonCounts` 对各原因分别计数。

批次级原因：`unknown_contract`（版本未注册）、`contract_deprecated`（版本被废弃）、
`contract_change_rejected`（按策略被兼容决策拒绝）。

## 契约演进与兼容性语义

`contract.Diff(old, new)` 产生稳定排序的字段级变更：新增必填/可选字段、删除字段、
类型变更、必填⇄可选、数值约束收紧/放宽、键组变更。

- **破坏性变更**（Breaking）：新增必填字段、删除字段、类型变更、可选转必填、
  约束收紧、键组变更。
- **兼容变更**：新增可选字段、必填转可选、约束放宽。

处理策略 `contract.Policy`（默认 `DefaultPolicy()`）：

| 场景 | 默认模式 | 语义 |
|---|---|---|
| 破坏性变更 | `reject` | 批次拒绝，**不**推进水位/基线，生效版本不变 |
| 兼容变更 | `alert` | 正常接收，发 `contract_compatible_change` 告警，生效版本升级 |
| 提交更旧版本 | `alert` | 正常接收但发 `contract_downgrade`，生效版本不变、基线不重放 |

`ModeSideways`（旁路）下批次**单独计数**（`sideways_batches`），
既不进入主水位/基线，也不升级生效版本，并带 `contract_breaking_change` 告警。
被 `reject` 的批次其结论同样被缓存：同一 BatchID 重放仍返回该首次结论（标记 duplicate）。

## 水位、乱序与重复语义

每个批次携带业务**事件时间** `EventTimeMillis`；监控器维护事件时间水位 `watermark`。

- **水位只进不退**，且只被「完全合法、非迟到」的批次推进：
  - 含非法记录的被拒批次不能用其事件时间污染水位；
  - 乱序批次的较小事件时间不会拉低水位。
- **迟到判定**：`eventTime + AllowedLatenessMs < watermark` 即为迟到，发 `late_batch`。
  迟到批次的记录**仍计入**记录计数与完整性（数据不静默丢弃），
  但不推进水位、不更新分布基线。
- **重复批次（BatchID 相同）**：在引擎临界区命中幂等缓存，直接返回**首次结论的深拷贝**，
  `outcome=duplicate` 并附 `duplicate_batch` 告警；任何计数器、水位、基线都不二次变化。
  重复判定先于监控应用，因此重复提交天然并发安全。

## 指标定义（Snapshot）

| 指标 | 定义 |
|---|---|
| `watermark_ms` | 已见「干净」批次事件时间的高水位（单调不减） |
| `last_seen_ms` | 最近一次提交的引擎接收时间 |
| `total_accepted_records` / `total_rejected_records` | 累计合法/非法记录数（迟到、被拒批次中的记录也分类计入，不丢弃） |
| `accepted_batches` / `rejected_batches` | 无非法记录 / 含非法记录的唯一批次数；另有 `sideways_batches` |
| `duplicate_batches` | 命中幂等缓存的重复提交次数（仅观测，不计入业务量） |
| `completeness_ratio` | 窗口内合法记录 /（合法+非法记录）；`WindowMs=0` 时为全量 |
| `freshness_lag_ms` | 墙钟时间 − 水位；超过 `FreshnessAlertMs` 发 `freshness` 告警 |
| `distributions[field]` | 每个数值字段的基线：Welford 在线均值/方差、Min/Max、冻结时刻 |
| `active_version` | 当前生效契约版本 |

### 分布漂移基线

- 用 **Welford 在线算法**累积均值与样本方差（数值稳定、单次 O(1)）。
- 收集满 `FreezeAfter`（默认 200）条**合法记录后冻结**，此后基线不再被后续数据改变，
  因此基线不会「倒退」或被异常分布带偏。
- 冻结后对每个批次计算批均值的 **z-score**：`|batchMean − baselineMean| / stdDev > DriftZ`
  （默认 3.0）时发 `distribution_drift` 告警。
- 迟到批次不参与基线累积，避免乱序数据污染。

## 并发保证

1. **快照语义**：去重检查、契约版本决策、监控指标应用在引擎的**同一互斥临界区**内完成。
   返回的 `BatchResult.MetricsAfter` 与该批次的结论是一个原子一致视图，
   不会出现「结果显示已计数但快照缺失」或反之的中间态。
2. **快照隔离**：`Snapshot()` 与重复批次返回值均为**深拷贝**，调用方修改不影响引擎内部。
3. **无丢失更新**：监控状态用互斥锁串行化，高并发下计数、水位精确（测试以 10×100、
   16×50 重复提交在 `-race` 下验证）。
4. **故障隔离**：
   - 每条记录的全部自定义规则在**独立 goroutine** 中执行，外层 `recover` 捕获 panic，
     转成 `custom_rule_panic` 原因；
   - 超过 `RuleTimeout` 转成 `custom_rule_timeout`，校验线程不被长时间阻塞；
   - panic/超时只影响当前记录，引擎状态不被破坏，其它批次正常处理，
     底层堆栈不向调用方泄漏（仅以原因码 + 摘要字符串呈现）。
   注意：超时无法强制杀死持续运行的用户 goroutine（Go 限制），但引擎不等待它。
5. **校验在锁外执行**：CPU 密集/可能超时的校验不阻塞其他提交者读取监控状态；
   只有最终状态提交是串行的。

## 批次结论（Outcome）

| Outcome | 含义 |
|---|---|
| `accepted` | 批次所有记录合法，已计入监控（迟到的合法批次仍为 accepted，但不推进水位/基线） |
| `rejected` | 存在非法记录，或未知/废弃契约，或破坏性演进被 reject |
| `duplicate` | BatchID 已处理过，返回首次结论快照，不计数 |
| `sideways` | 结构不兼容被旁路，单独计数，不进主基线 |

## 验证方法

```bash
# 全量单元测试 + 竞态检测
go test ./... -race -count=1

# 仅端到端集成场景（内存数据源 -> 引擎，逆序乱序投喂 + 重复 + 演进 + 畸形）
go test ./internal/engine -race -run TestEndToEnd -v

# 静态检查与格式
go vet ./...
gofmt -l .

# 可运行演示
go run ./cmd/demo
```

测试覆盖矩阵：

- 并发提交一致性、并发重复批次仅计数一次（`-race`）；
- 重复批次幂等、乱序/迟到批次不使水位倒退、不污染基线；
- 契约兼容演进（告警并升级）、破坏性演进（拒绝 / 旁路）、旧版本降级；
- 畸形输入：null 记录、缺字段、类型冲突、越界、键组重复；
- 自定义规则 panic / 超时 / 拒绝的隔离与可恢复性；
- 基线冻结、漂移告警、旁路不进主基线、快照深拷贝隔离。

单测日志使用标准 `log` 包打印**批次标识、规则版本、关键指标（水位/完整性/计数）
与判定原因/告警类型**，见 `internal/engine/*_test.go` 中的 `logResult` 与集成测试输出。
