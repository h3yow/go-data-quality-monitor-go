package dq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Engine 是数据质量校验与增量监控引擎的入口。
// 并发提交安全：每个批次在提交瞬间获取契约的一致性快照，
// 校验与监控均基于该快照，互不阻塞(监控状态由内部锁保护)。
type Engine struct {
	mu       sync.RWMutex
	contract Contract
	compat   CompatMode
	custom   []CustomValidator
	timeout  time.Duration
	warnings []string

	monitor *Monitor
}

// Option 配置引擎。
type Option func(*Engine)

// WithCompatMode 设置契约演进的兼容性处理策略(默认 CompatReject)。
func WithCompatMode(m CompatMode) Option {
	return func(e *Engine) { e.compat = m }
}

// WithCustomValidators 追加自定义校验逻辑。
func WithCustomValidators(vs ...CustomValidator) Option {
	return func(e *Engine) { e.custom = append(e.custom, vs...) }
}

// WithValidatorTimeout 设置单条记录自定义校验的总时限(默认 1s)。
func WithValidatorTimeout(d time.Duration) Option {
	return func(e *Engine) { e.timeout = d }
}

// WithClock 注入时钟(用于新鲜度计算)，主要供测试使用。
func WithClock(now func() time.Time) Option {
	return func(e *Engine) { e.monitor.now = now }
}

// NewEngine 创建引擎，initial 为初始契约(版本建议从 1 起)。
func NewEngine(initial Contract, opts ...Option) *Engine {
	e := &Engine{
		contract: initial,
		timeout:  time.Second,
		monitor:  NewMonitor(),
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Submit 提交一个批次进行校验与监控，返回校验结论。
// 重复提交相同 Batch.ID 的批次会重新校验(结论可复现)，
// 但监控指标只会计入一次。任何内部故障都会以 error 返回，
// 不会向调用方泄漏 panic。
func (e *Engine) Submit(ctx context.Context, b Batch) (res ValidationResult, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("dq: internal fault while processing batch %q: %v", b.ID, p)
			res = ValidationResult{BatchID: b.ID}
		}
	}()
	if err := ctx.Err(); err != nil {
		return ValidationResult{BatchID: b.ID}, err
	}

	// 获取契约一致性快照。
	e.mu.RLock()
	c := e.contract
	custom := append([]CustomValidator(nil), e.custom...)
	timeout := e.timeout
	e.mu.RUnlock()

	v := NewValidator(c, custom, timeout)
	res = v.Validate(b)
	e.monitor.Observe(b, res, c)
	return res, nil
}

// Evolve 将契约升级到新版本，行为同 EvolveWithReport，但不返回报告。
func (e *Engine) Evolve(c Contract) error {
	_, err := e.EvolveWithReport(c)
	return err
}

// EvolveWithReport 将契约升级到新版本，并返回逐项可解释的兼容性报告。
// 新版本号必须严格大于当前版本。兼容性按引擎策略处理：
//   - CompatReject: 不兼容时返回错误，契约保持不变；
//   - CompatBypass: 不兼容时忽略本次演进(旁路)，返回 nil；
//   - CompatWarn:   不兼容时仍应用新版本，并记录告警。
//
// 无论策略如何，返回的报告都完整描述每一项变化及其兼容性判定。
func (e *Engine) EvolveWithReport(c Contract) (CompatibilityReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if c.Version <= e.contract.Version {
		return CompatibilityReport{FromVersion: e.contract.Version, ToVersion: c.Version},
			fmt.Errorf("dq: contract version must increase (current %d, got %d)",
				e.contract.Version, c.Version)
	}
	rep := AnalyzeCompatibility(e.contract, c)
	if !rep.Compatible() {
		incompat := rep.Incompatible()
		switch e.compat {
		case CompatReject:
			return rep, fmt.Errorf("dq: incompatible contract evolution rejected: %v", incompat)
		case CompatBypass:
			e.warnings = append(e.warnings,
				fmt.Sprintf("contract v%d bypassed due to incompatibility: %v", c.Version, incompat))
			return rep, nil
		case CompatWarn:
			e.warnings = append(e.warnings,
				fmt.Sprintf("contract v%d applied despite incompatibility: %v", c.Version, incompat))
		}
	}
	e.contract = c
	return rep, nil
}

// CheckCompatibility 返回从 old 演进到 next 的不兼容项列表。
// 兼容的变化：新增可选字段、必需放宽为可选、约束放宽或移除。
// 不兼容的变化：删除字段、字段类型变化、新增必需字段、可选收紧为必需、
// 新增约束或约束收窄、键组变化。
func CheckCompatibility(old, next Contract) []string {
	rep := AnalyzeCompatibility(old, next)
	var out []string
	for _, ch := range rep.Incompatible() {
		out = append(out, ch.Detail)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Metrics 返回当前监控指标的一致性快照。
func (e *Engine) Metrics() Metrics {
	return e.monitor.Snapshot()
}

// SeenBatch 报告批次 ID 是否已被监控计入(重复提交会被幂等忽略)。
func (e *Engine) SeenBatch(id string) bool {
	return e.monitor.Seen(id)
}

// Warnings 返回引擎级告警(如契约演进告警)的副本。
func (e *Engine) Warnings() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]string(nil), e.warnings...)
}

// Contract 返回当前契约快照。
func (e *Engine) Contract() Contract {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.contract
}
