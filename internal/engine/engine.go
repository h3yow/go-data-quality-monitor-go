// Package engine 编排契约校验与增量监控，提供并发提交、故障隔离与快照语义。
package engine

import (
	"sync"

	"github.com/h3yow/go-data-quality-monitor-go/internal/contract"
	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
	"github.com/h3yow/go-data-quality-monitor-go/internal/monitor"
	"github.com/h3yow/go-data-quality-monitor-go/internal/validate"
)

// Options 控制引擎行为。
type Options struct {
	// DataSet 名称；默认 "default"。
	DataSet string
	// Policy 结构兼容性变化策略。
	Policy contract.Policy
	// Validate 校验器选项（自定义规则、超时）。
	Validate validate.Options
	// Monitor 监控器选项。
	Monitor monitor.Options
	// NowFn 注入时钟（毫秒 epoch），nil 表示墙钟。
	NowFn func() int64
}

// datasetState 是单个数据集的全部可变状态，由 mu 串行化，提供快照语义。
type datasetState struct {
	monitor    monitor.Monitor
	active     model.Contract
	hasActive  bool
	seen       map[string]model.BatchResult // BatchID -> 首次结论
	duplicates int64
}

// Engine 是可嵌入的数据质量引擎。
type Engine struct {
	opts      Options
	registry  contract.Registry
	validator validate.Validator

	mu    sync.Mutex
	state map[string]*datasetState
}

// New 创建引擎。
func New(opts Options) *Engine {
	if opts.DataSet == "" {
		opts.DataSet = "default"
	}
	p := opts.Policy
	if p.OnBreaking == "" {
		p.OnBreaking = contract.ModeReject
	}
	if p.OnCompatible == "" {
		p.OnCompatible = contract.ModeAlert
	}
	if p.OnDowngrade == "" {
		p.OnDowngrade = contract.ModeAlert
	}
	opts.Policy = p
	if opts.NowFn == nil {
		opts.NowFn = monitorNow
	}
	return &Engine{
		opts:      opts,
		registry:  contract.NewRegistry(),
		validator: validate.NewValidator(),
		state:     map[string]*datasetState{},
	}
}

// Registry 暴露契约注册表（可用于测试/嵌入方查询版本）。
func (e *Engine) Registry() contract.Registry { return e.registry }

// stateFor 返回（或惰性创建）数据集状态；调用方须持有 e.mu。
func (e *Engine) stateFor(name string) *datasetState {
	st, ok := e.state[name]
	if !ok {
		st = &datasetState{
			monitor: monitor.New(name, e.opts.Monitor, e.opts.NowFn),
			seen:    map[string]model.BatchResult{},
		}
		e.state[name] = st
	}
	return st
}

// RegisterContract 注册一个契约版本（按语义化版本排序，最高版本生效）。
func (e *Engine) RegisterContract(c model.Contract) error {
	return e.registry.Register(c)
}

// Snapshot 返回指定数据集监控状态的一致快照（深拷贝）。
func (e *Engine) Snapshot(contractName string) (*model.Snapshot, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.state[contractName]
	if !ok {
		return nil, false
	}
	return st.monitor.Snapshot(), true
}

// Submit 提交一个批次，返回完整且可解释的判定结论。
// 并发安全；自定义规则的 panic/超时在 validator 内被隔离，不会破坏引擎状态。
//
// 快照语义：去重检查、版本兼容决策、监控指标应用在同一临界区内完成，
// 因此 BatchResult.MetricsAfter 与该批次的结果是一致的原子视图。
func (e *Engine) Submit(b model.Batch) model.BatchResult {
	seenAt := e.opts.NowFn()

	// 1) 解析契约版本（只读注册表，无需进入引擎临界区）。
	c, ok := e.registry.Get(e.opts.DataSet, b.ContractV)
	if !ok {
		return model.BatchResult{
			BatchID: b.BatchID, ContractV: b.ContractV,
			Outcome:      model.OutcomeRejected,
			Reasons:      []model.Reason{model.BatchReasonUnknownContract},
			ReasonCounts: map[model.Reason]int{model.BatchReasonUnknownContract: 1},
			Total:        len(b.Records),
			EventTimeMs:  b.EventTimeMillis, ProcessedMs: e.opts.NowFn(),
		}
	}

	// 2) 校验在锁外执行（CPU 密集/可能被超时的自定义规则不阻塞其它批次的状态访问）。
	recordResults := e.validator.Validate(c, b, e.opts.Validate)
	valid, invalid := countResults(recordResults)
	reasonCounts := tallyReasons(recordResults)

	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.stateFor(c.Name)

	// 3) 重复批次：直接返回首次结论的深拷贝快照，绝不重复计数。
	if first, dup := st.seen[b.BatchID]; dup {
		cp := cloneResult(first)
		cp.Outcome = model.OutcomeDuplicate
		cp.Alerts = append(cp.Alerts, model.Alert{
			BatchID: b.BatchID, ContractV: b.ContractV, Kind: "duplicate_batch",
			Detail:      "batch already processed; returning first verdict",
			EventTimeMs: b.EventTimeMillis, SeenTimeMs: seenAt,
		})
		cp.ProcessedMs = e.opts.NowFn()
		st.duplicates++
		return cp
	}

	// 4) 契约演进与兼容策略决策。
	verdict := e.resolveVersionLocked(st, c, b, seenAt)

	result := model.BatchResult{
		BatchID:       b.BatchID,
		ContractName:  c.Name,
		ContractV:     c.Version,
		Outcome:       model.OutcomeRejected,
		RecordResults: recordResults,
		ReasonCounts:  reasonCounts,
		Total:         len(b.Records),
		ValidCount:    valid,
		InvalidCount:  invalid,
		EventTimeMs:   b.EventTimeMillis,
		ProcessedMs:   e.opts.NowFn(),
	}
	for r, n := range reasonCounts {
		for i := 0; i < n; i++ {
			result.Reasons = append(result.Reasons, r)
		}
	}
	result.Alerts = append(result.Alerts, verdict.alerts...)

	switch verdict.mode {
	case contract.ModeReject:
		result.Outcome = model.OutcomeRejected
		result.Reasons = append(result.Reasons, model.BatchReasonContractRejected)
		if result.ReasonCounts == nil {
			result.ReasonCounts = map[model.Reason]int{}
		}
		result.ReasonCounts[model.BatchReasonContractRejected]++
		st.seen[b.BatchID] = cloneResult(result)
		result.MetricsAfter = st.monitor.Snapshot()
		return result
	case contract.ModeSideways:
		ap := st.monitor.Apply(monitor.ApplyRequest{
			Batch: b, ContractName: c.Name, ValidCount: valid, InvalidCount: invalid,
			ReasonCounts: reasonCounts, RecordResults: recordResults,
			Sideways: true, SeenTimeMs: seenAt,
		})
		result.Outcome = model.OutcomeSideways
		result.MetricsAfter = ap.Snapshot
		st.seen[b.BatchID] = cloneResult(result)
		return result
	default:
		// ModeAlert / 无变化：进入正常监控流水线。
		if !st.hasActive {
			st.hasActive = true
			st.active = c
			st.monitor.SetActiveVersion(c.Version)
		} else if verdict.promote {
			st.active = c
			st.monitor.SetActiveVersion(c.Version)
		}
		ap := st.monitor.Apply(monitor.ApplyRequest{
			Batch: b, ContractName: c.Name, ValidCount: valid, InvalidCount: invalid,
			ReasonCounts: reasonCounts, RecordResults: recordResults,
			Sideways: false, SeenTimeMs: seenAt,
		})
		result.Outcome = ap.Outcome
		result.Alerts = append(result.Alerts, ap.Alerts...)
		result.MetricsAfter = ap.Snapshot
		st.seen[b.BatchID] = cloneResult(result)
		return result
	}
}

// versionVerdict 描述契约兼容决策结果。
type versionVerdict struct {
	mode    contract.CompatMode
	promote bool // 是否将该版本提升为生效版本
	alerts  []model.Alert
}

// resolveVersionLocked 在临界区内决定该批次的契约兼容处理方式。调用方持有 e.mu。
func (e *Engine) resolveVersionLocked(st *datasetState, c model.Contract, b model.Batch, seenAt int64) versionVerdict {
	if c.Deprecated {
		return versionVerdict{mode: contract.ModeReject, alerts: []model.Alert{{
			BatchID: b.BatchID, ContractV: c.Version, Kind: "contract_deprecated",
			Detail: "contract version is deprecated", EventTimeMs: b.EventTimeMillis, SeenTimeMs: seenAt,
		}}}
	}
	if !st.hasActive {
		return versionVerdict{mode: contract.ModeAlert, promote: true}
	}
	active := st.active
	if active.Version == c.Version {
		return versionVerdict{mode: contract.ModeAlert}
	}
	cmp := contract.CompareVersions(c.Version, active.Version)
	if cmp < 0 {
		// 旧版本：按降级策略处理，不改变生效版本、不重放基线。
		return versionVerdict{mode: e.opts.Policy.OnDowngrade, alerts: []model.Alert{{
			BatchID: b.BatchID, ContractV: c.Version, Kind: "contract_downgrade",
			Detail:      "batch uses version older than active " + active.Version,
			EventTimeMs: b.EventTimeMillis, SeenTimeMs: seenAt,
		}}}
	}
	changes := contract.Diff(active, c)
	breaking := false
	detail := "contract evolved compatibly"
	for _, ch := range changes {
		if ch.Breaking {
			breaking = true
			detail = "breaking change: " + ch.Detail
			break
		}
	}
	if breaking {
		mode := e.opts.Policy.OnBreaking
		v := versionVerdict{mode: mode, promote: mode == contract.ModeAlert}
		if mode != contract.ModeReject {
			v.alerts = []model.Alert{{
				BatchID: b.BatchID, ContractV: c.Version, Kind: "contract_breaking_change",
				Detail: detail, EventTimeMs: b.EventTimeMillis, SeenTimeMs: seenAt,
			}}
		}
		return v
	}
	return versionVerdict{mode: e.opts.Policy.OnCompatible, promote: true, alerts: []model.Alert{{
		BatchID: b.BatchID, ContractV: c.Version, Kind: "contract_compatible_change",
		Detail: detail, EventTimeMs: b.EventTimeMillis, SeenTimeMs: seenAt,
	}}}
}

func countResults(rr []model.RecordResult) (valid, invalid int) {
	for _, r := range rr {
		if r.Valid {
			valid++
		} else {
			invalid++
		}
	}
	return valid, invalid
}

func tallyReasons(rr []model.RecordResult) map[model.Reason]int {
	counts := map[model.Reason]int{}
	for _, r := range rr {
		// 一条记录的多种违规全部计数（原因可区分、不静默合并）。
		for _, v := range r.Violations {
			counts[v.Reason]++
		}
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

// cloneResult 深拷贝批次结论，使缓存与返回值互不影响（快照隔离）。
func cloneResult(r model.BatchResult) model.BatchResult {
	cp := r
	if r.ReasonCounts != nil {
		cp.ReasonCounts = make(map[model.Reason]int, len(r.ReasonCounts))
		for k, v := range r.ReasonCounts {
			cp.ReasonCounts[k] = v
		}
	}
	if r.Reasons != nil {
		cp.Reasons = append([]model.Reason(nil), r.Reasons...)
	}
	if r.RecordResults != nil {
		cp.RecordResults = append([]model.RecordResult(nil), r.RecordResults...)
		for i := range cp.RecordResults {
			if r.RecordResults[i].Violations != nil {
				cp.RecordResults[i].Violations = append([]model.Violation(nil), r.RecordResults[i].Violations...)
			}
		}
	}
	if r.Alerts != nil {
		cp.Alerts = append([]model.Alert(nil), r.Alerts...)
	}
	cp.MetricsAfter = r.MetricsAfter.Clone()
	return cp
}
