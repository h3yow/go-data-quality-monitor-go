package dq

import (
	"context"
	"fmt"
	"math/rand"
)

// ReplayEvent 是回放脚本中的一个事件：提交批次或推进契约版本。
// Batch 与 Contract 恰好设置一个。
type ReplayEvent struct {
	Batch    *Batch    // 提交一个批次(可乱序/重复/迟到)
	Contract *Contract // 推进契约到该版本(按引擎兼容策略处理)
}

// SubmitEvent 构造一个批次提交事件。
func SubmitEvent(b Batch) ReplayEvent { return ReplayEvent{Batch: &b} }

// EvolveEvent 构造一个契约演进事件。
func EvolveEvent(c Contract) ReplayEvent { return ReplayEvent{Contract: &c} }

// BatchOutcome 是回放中单个批次提交的可解释结论。
type BatchOutcome struct {
	BatchID         string
	Seq             uint64
	ContractVersion int // 该批次提交瞬间生效的契约版本
	Accepted        int // 接受记录数
	Violations      []Violation
	Duplicate       bool  // 监控侧是否按重复批次幂等忽略
	Err             error // 提交故障(正常为 nil)
}

// EvolutionOutcome 是回放中一次契约演进的结论。
type EvolutionOutcome struct {
	ToVersion int
	Report    CompatibilityReport // 逐项兼容性判定
	Applied   bool                // 新版本是否生效(旁路/拒绝时为 false)
	Err       error               // 拒绝原因(CompatReject 且不兼容时非 nil)
}

// ReplayReport 是一次完整回放的结论。
type ReplayReport struct {
	Outcomes   []BatchOutcome     // 按回放顺序的逐批结论
	Evolutions []EvolutionOutcome // 按回放顺序的演进结论
	Timeline   []string           // 按真实发生顺序的逐事件日志行
	Metrics    Metrics            // 回放结束后的最终指标
	Warnings   []string           // 引擎告警(如演进告警)
}

// Replayer 在本地无外部依赖地回放多批次数据流。
// 回放等价于按脚本顺序手动调用 Engine.Submit/EvolveWithReport，
// 结论与手动逐批提交完全一致；同一脚本重复回放结论一致。
type Replayer struct {
	engine *Engine
}

// NewReplayer 创建回放器，initial 为初始契约，opts 透传给引擎。
func NewReplayer(initial Contract, opts ...Option) *Replayer {
	return &Replayer{engine: NewEngine(initial, opts...)}
}

// Engine 返回底层引擎，便于交错手动提交/演进。
func (r *Replayer) Engine() *Engine { return r.engine }

// Run 按顺序回放事件脚本，返回完整结论。
// 每个批次按提交瞬间的契约版本判定；演进事件按引擎兼容策略处理，
// 被拒绝或旁路的演进不影响后续批次。
func (r *Replayer) Run(events []ReplayEvent) ReplayReport {
	rep := ReplayReport{}
	ctx := context.Background()
	for _, ev := range events {
		switch {
		case ev.Batch != nil:
			out := r.submitOne(ctx, *ev.Batch)
			rep.Outcomes = append(rep.Outcomes, out)
			rep.Timeline = append(rep.Timeline, out.String())
		case ev.Contract != nil:
			out := r.evolveOne(*ev.Contract)
			rep.Evolutions = append(rep.Evolutions, out)
			rep.Timeline = append(rep.Timeline, out.String())
		}
	}
	rep.Metrics = r.engine.Metrics()
	rep.Warnings = r.engine.Warnings()
	return rep
}

// RunBatches 是 Run 的便捷形式：仅按给定顺序回放批次。
func (r *Replayer) RunBatches(batches []Batch) ReplayReport {
	events := make([]ReplayEvent, 0, len(batches))
	for _, b := range batches {
		events = append(events, SubmitEvent(b))
	}
	return r.Run(events)
}

// String 返回该批次结论的单行日志形式。
func (o BatchOutcome) String() string {
	status := "ok"
	if o.Duplicate {
		status = "duplicate(ignored by monitor)"
	}
	if o.Err != nil {
		status = "error: " + o.Err.Error()
	}
	return fmt.Sprintf("batch %s seq=%d contract=v%d accepted=%d violations=%d %s",
		o.BatchID, o.Seq, o.ContractVersion, o.Accepted, len(o.Violations), status)
}

// String 返回该演进结论的单行日志形式。
func (o EvolutionOutcome) String() string {
	return fmt.Sprintf("evolve -> v%d applied=%v compatible=%v err=%v",
		o.ToVersion, o.Applied, o.Report.Compatible(), o.Err)
}

// submitOne 提交单个批次并记录结论。
func (r *Replayer) submitOne(ctx context.Context, b Batch) BatchOutcome {
	out := BatchOutcome{BatchID: b.ID, Seq: b.Seq}
	out.Duplicate = r.engine.SeenBatch(b.ID)
	res, err := r.engine.Submit(ctx, b)
	if err != nil {
		out.Err = err
		return out
	}
	out.ContractVersion = res.ContractVersion
	out.Accepted = len(res.Accepted)
	out.Violations = res.Violations
	return out
}

// evolveOne 推进契约版本并记录结论。
func (r *Replayer) evolveOne(c Contract) EvolutionOutcome {
	out := EvolutionOutcome{ToVersion: c.Version}
	rep, err := r.engine.EvolveWithReport(c)
	out.Report = rep
	out.Err = err
	out.Applied = err == nil && r.engine.Contract().Version == c.Version
	return out
}

// Permute 返回批次的确定性乱序副本：同一 seed 必得同一顺序，
// 用于复现乱序到达场景。
func Permute(batches []Batch, seed int64) []Batch {
	out := append([]Batch(nil), batches...)
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// Redeliver 返回追加了指定批次 ID 重复投递的副本(保持原顺序，
// 重复投递按 ids 顺序附加到末尾)，用于复现同一批被重复提交的场景。
func Redeliver(batches []Batch, ids ...string) []Batch {
	out := append([]Batch(nil), batches...)
	for _, id := range ids {
		for _, b := range batches {
			if b.ID == id {
				out = append(out, b)
				break
			}
		}
	}
	return out
}

// DeferLast 返回把指定批次 ID 移到末尾的副本(其余保持原顺序)，
// 用于复现迟到批次场景。
func DeferLast(batches []Batch, ids ...string) []Batch {
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	var head, tail []Batch
	for _, b := range batches {
		if _, ok := want[b.ID]; ok {
			tail = append(tail, b)
		} else {
			head = append(head, b)
		}
	}
	return append(head, tail...)
}
