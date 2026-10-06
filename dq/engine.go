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
		contract: cloneContract(initial),
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

	// 获取契约一致性快照(深拷贝，演进不会造成半新半旧判定)。
	e.mu.RLock()
	c := cloneContract(e.contract)
	custom := append([]CustomValidator(nil), e.custom...)
	timeout := e.timeout
	e.mu.RUnlock()

	v := NewValidator(c, custom, timeout)
	res = v.Validate(b)
	e.monitor.Observe(b, res, c)
	return res, nil
}

// Evolve 将契约升级到新版本。
// 新版本号必须严格大于当前版本。兼容性按引擎策略处理：
//   - CompatReject: 不兼容时返回错误，契约保持不变；
//   - CompatBypass: 不兼容时忽略本次演进(旁路)，返回 nil；
//   - CompatWarn:   不兼容时仍应用新版本，并记录告警。
func (e *Engine) Evolve(c Contract) error {
	return e.EvolveDetailed(c).Err
}

// CheckCompatibility 返回从 old 演进到 next 的不兼容项描述列表。
// 判定细节见 AnalyzeEvolution：除既有的删除字段/类型变化/新增必需字段/
// 键组变化外，还包括必填收紧(可选 -> 必填)与约束收窄(min/max/maxLen)；
// 放宽约束、必填放松为可选不算不兼容。
func CheckCompatibility(old, next Contract) []string {
	var out []string
	for _, c := range AnalyzeEvolution(old, next) {
		if !c.Compatible {
			out = append(out, c.String())
		}
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
