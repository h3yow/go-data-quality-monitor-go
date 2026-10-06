package dq

import (
	"context"
)

// StepKind 标识回放脚本中单步操作的类型。
type StepKind string

const (
	StepSubmit StepKind = "submit" // 提交一个批次
	StepEvolve StepKind = "evolve" // 尝试一次契约演进
)

// Step 是回放脚本中的一步：要么提交批次，要么演进契约。
// 回放器严格按切片顺序执行，因此：
//   - 乱序批次：让 Step.Batch.Seq 不按递增顺序出现即可；
//   - 重复批次：重复放置同一 ID/Seq 的提交步；
//   - 迟到批次：在更大 Seq 的批次之后再放入较小 Seq 的批次；
//   - 演进交错：把 StepEvolve 穿插在提交步之间。
type Step struct {
	Kind     StepKind
	Label    string   // 可选说明，便于日志阅读
	Batch    Batch    // Kind == StepSubmit 时使用
	Contract Contract // Kind == StepEvolve 时使用
}

// StepResult 是单步回放的结论。Validation 与 Evolution 互斥非空，
// 分别对应提交步与演进步。
type StepResult struct {
	Index      int
	Label      string
	Kind       StepKind
	BatchID    string
	Seq        uint64
	Validation *ValidationResult
	Evolution  *EvolutionReport
	Err        error
}

// ReplayReport 是整段回放的汇总：逐步结论 + 最终指标/契约/告警。
type ReplayReport struct {
	Steps           []StepResult
	Metrics         Metrics
	Warnings        []string
	ContractVersion int
}

// Replayer 在本地把预定义脚本按顺序当作多批次数据流重放，
// 全程不依赖数据库或消息队列。所有步骤共享同一个 Engine，
// 走的就是 Engine.Submit / Engine.EvolveDetailed 的同一执行路径，
// 因此回放结论与手动逐批调用完全一致、可逐字节复现。
type Replayer struct {
	engine  *Engine
	results []StepResult
}

// NewReplayer 以初始契约与引擎选项创建回放器。
func NewReplayer(initial Contract, opts ...Option) *Replayer {
	return &Replayer{engine: NewEngine(cloneContract(initial), opts...)}
}

// Engine 返回底层引擎，便于随时读取指标或手动操作。
func (r *Replayer) Engine() *Engine { return r.engine }

// SubmitStep 提交一个批次并记录该步结论。
func (r *Replayer) SubmitStep(ctx context.Context, b Batch, label string) StepResult {
	sr := StepResult{
		Index:   len(r.results),
		Label:   label,
		Kind:    StepSubmit,
		BatchID: b.ID,
		Seq:     b.Seq,
	}
	res, err := r.engine.Submit(ctx, b)
	sr.Validation = &res
	sr.Err = err
	r.results = append(r.results, sr)
	return sr
}

// EvolveStep 执行一次契约演进并记录该步结论。
func (r *Replayer) EvolveStep(c Contract, label string) StepResult {
	rep := r.engine.EvolveDetailed(c)
	sr := StepResult{
		Index:     len(r.results),
		Label:     label,
		Kind:      StepEvolve,
		Evolution: &rep,
		Err:       rep.Err,
	}
	r.results = append(r.results, sr)
	return sr
}

// Run 按切片顺序逐步执行脚本并返回完整回放报告。
// 执行过程中任一步返回错误都不会中断(演进被拒绝、批次提交失败本身
// 就是要观察的结论)；仅在 ctx 已取消时停止后续步骤。
func (r *Replayer) Run(ctx context.Context, steps []Step) ReplayReport {
	for _, s := range steps {
		if err := ctx.Err(); err != nil {
			break
		}
		switch s.Kind {
		case StepEvolve:
			r.EvolveStep(s.Contract, s.Label)
		default:
			r.SubmitStep(ctx, s.Batch, s.Label)
		}
	}
	return r.Report()
}

// Report 返回截至当前的回放汇总快照。
func (r *Replayer) Report() ReplayReport {
	return ReplayReport{
		Steps:           r.Results(),
		Metrics:         r.engine.Metrics(),
		Warnings:        r.engine.Warnings(),
		ContractVersion: r.engine.Contract().Version,
	}
}

// Results 返回截至当前的逐步结论副本。
func (r *Replayer) Results() []StepResult {
	out := make([]StepResult, len(r.results))
	copy(out, r.results)
	return out
}
