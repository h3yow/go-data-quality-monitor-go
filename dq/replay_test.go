package dq

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func replayContract(version int, maxLen int) Contract {
	return Contract{
		Version: version,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "amount", Type: TypeFloat, Required: true},
			{Name: "tag", Type: TypeString, Required: false, Constraint: &Constraint{MaxLen: &maxLen}},
		},
		KeyGroup: []string{"id"},
	}
}

func mkBatch(id string, seq uint64, base time.Time, recs ...Record) Batch {
	return Batch{ID: id, Seq: seq, Records: recs, EventTime: base.Add(time.Duration(seq) * time.Minute)}
}

func logStep(t *testing.T, s StepResult) {
	switch s.Kind {
	case StepSubmit:
		v := s.Validation
		t.Logf("step=%d submit batch=%s seq=%d rule=v%d accepted=%d violations=%d label=%q",
			s.Index, s.BatchID, s.Seq, v.ContractVersion, len(v.Accepted), len(v.Violations), s.Label)
		for _, viol := range v.Violations {
			t.Logf("    row=%d reason=%s field=%s detail=%s", viol.Row, viol.Reason, viol.Field, viol.Detail)
		}
	case StepEvolve:
		r := s.Evolution
		t.Logf("step=%d evolve v%d->v%d action=%s label=%q", s.Index, r.FromVersion, r.ToVersion, r.Action, s.Label)
		for _, c := range r.Changes {
			t.Logf("    compatible=%v %s", c.Compatible, c)
		}
	}
}

func logMetrics(t *testing.T, m Metrics) {
	t.Logf("METRICS watermark=%d seen=%d accepted=%d violations=%d completeness=%v lag=%s",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations, m.Completeness, m.FreshnessLag)
}

// steps 定义一段包含乱序、重复、迟到的多批次脚本。
func replaySteps(base time.Time) []Step {
	return []Step{
		{Kind: StepSubmit, Label: "首批 seq=1", Batch: mkBatch("r1", 1, base,
			Record{"id": 1, "amount": 1.0, "tag": "a"},
			Record{"id": 2, "amount": 2.0, "tag": "bb"},
		)},
		{Kind: StepSubmit, Label: "乱序 seq=3 (缺口 2)", Batch: mkBatch("r3", 3, base,
			Record{"id": 3, "amount": 3.0, "tag": "ccc"},
			Record{"id": 4, "tag": "dddd"}, // amount 缺失
		)},
		{Kind: StepSubmit, Label: "重复投递 r1", Batch: mkBatch("r1", 1, base,
			Record{"id": 1, "amount": 1.0, "tag": "a"},
			Record{"id": 2, "amount": 2.0, "tag": "bb"},
		)},
		{Kind: StepSubmit, Label: "补齐 seq=2", Batch: mkBatch("r2", 2, base,
			Record{"id": 5, "amount": 5.0, "tag": "too-long-tag"}, // maxlen 5 -> 违规
		)},
		{Kind: StepSubmit, Label: "乱序 seq=6 (缺口 4,5)", Batch: mkBatch("r6", 6, base,
			Record{"id": 6, "amount": 6.0, "tag": "six"},
		)},
		{Kind: StepSubmit, Label: "迟到 seq=4", Batch: mkBatch("r4", 4, base,
			Record{"id": 7, "amount": 7.0, "tag": "four"},
		)},
		{Kind: StepSubmit, Label: "迟到 seq=5, 补齐后水位到 6", Batch: mkBatch("r5", 5, base,
			Record{"id": 8, "amount": 8.0, "tag": "five"},
			Record{"id": 8, "amount": 8.0, "tag": "dup"}, // 批内键重复
		)},
	}
}

// TestReplayOutOfOrderDuplicateLate 回放乱序/重复/迟到批次。
func TestReplayOutOfOrderDuplicateLate(t *testing.T) {
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	fixedNow := base.Add(30 * time.Minute)
	r := NewReplayer(replayContract(1, 5), WithClock(func() time.Time { return fixedNow }))
	rep := r.Run(context.Background(), replaySteps(base))
	for _, s := range rep.Steps {
		logStep(t, s)
	}
	logMetrics(t, rep.Metrics)

	// 水位最终追到 6；6 个不同批次(重复 r1 只计一次)。
	if rep.Metrics.Watermark != 6 {
		t.Fatalf("watermark want 6, got %d", rep.Metrics.Watermark)
	}
	if rep.Metrics.SeenBatches != 6 {
		t.Fatalf("seen batches want 6 (duplicate counted once), got %d", rep.Metrics.SeenBatches)
	}
	// r1:2 接受; r3:1 接受 1 违规; r2: 长 tag 违规 0 接受; r6:1; r4:1; r5:1 接受 1 重复违规
	if rep.Metrics.TotalAccepted != 6 {
		t.Fatalf("accepted want 6, got %d", rep.Metrics.TotalAccepted)
	}
	if rep.Metrics.TotalViolations != 3 {
		t.Fatalf("violations want 3, got %d", rep.Metrics.TotalViolations)
	}
	// 重复批次重新校验结论一致(2 accepted)
	dup := rep.Steps[2]
	if dup.Validation == nil || len(dup.Validation.Accepted) != 2 || len(dup.Validation.Violations) != 0 {
		t.Fatalf("duplicate replay must reproduce 2 accepted, got %+v", dup.Validation)
	}
}

// TestReplayIntermediateWatermark 逐步提交观察中间水位：
// 缺口存在时不推进，缺口补齐一次性追上，重复批不影响水位。
func TestReplayIntermediateWatermark(t *testing.T) {
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	r := NewReplayer(replayContract(1, 5))
	ctx := context.Background()

	r.SubmitStep(ctx, mkBatch("w1", 1, base, Record{"id": 1, "amount": 1.0}), "seq=1")
	if w := r.Engine().Metrics().Watermark; w != 1 {
		t.Fatalf("after seq=1 watermark want 1, got %d", w)
	}
	r.SubmitStep(ctx, mkBatch("w3", 3, base, Record{"id": 3, "amount": 3.0}), "乱序 seq=3")
	if w := r.Engine().Metrics().Watermark; w != 1 {
		t.Fatalf("gap at seq=2 must hold watermark at 1, got %d", w)
	}
	r.SubmitStep(ctx, mkBatch("w1", 1, base, Record{"id": 1, "amount": 1.0}), "重复 seq=1")
	if w := r.Engine().Metrics().Watermark; w != 1 {
		t.Fatalf("duplicate batch must not move watermark, got %d", w)
	}
	r.SubmitStep(ctx, mkBatch("w2", 2, base, Record{"id": 2, "amount": 2.0}), "迟到 seq=2 补齐缺口")
	if w := r.Engine().Metrics().Watermark; w != 3 {
		t.Fatalf("filled gap must catch watermark up to 3, got %d", w)
	}
	t.Logf("中间水位: 1 ->(缺口保持)-> 1 ->(重复不变)-> 1 ->(补齐)-> 3, seen=%d",
		r.Engine().Metrics().SeenBatches)
}

// TestReplayDeterministic 同一脚本重复回放，逐步结论与最终指标完全一致。
func TestReplayDeterministic(t *testing.T) {
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	fixedNow := base.Add(30 * time.Minute)
	run := func() ReplayReport {
		r := NewReplayer(replayContract(1, 5), WithClock(func() time.Time { return fixedNow }))
		return r.Run(context.Background(), replaySteps(base))
	}
	a, b := run(), run()
	// 指针字段不可比，比较规范化后的结论。
	if !reflect.DeepEqual(normReport(a), normReport(b)) {
		t.Fatalf("replaying same script must yield identical results")
	}
	t.Logf("两次回放结论一致: steps=%d watermark=%d accepted=%d violations=%d",
		len(a.Steps), a.Metrics.Watermark, a.Metrics.TotalAccepted, a.Metrics.TotalViolations)
}

// TestReplayMatchesManualSubmit 回放结论必须与手动逐批提交一致。
func TestReplayMatchesManualSubmit(t *testing.T) {
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	fixedNow := base.Add(30 * time.Minute)
	clock := func() time.Time { return fixedNow }

	r := NewReplayer(replayContract(1, 5), WithClock(clock))
	rep := r.Run(context.Background(), replaySteps(base))

	e := NewEngine(replayContract(1, 5), WithClock(clock))
	var manual []StepResult
	for i, s := range replaySteps(base) {
		res, err := e.Submit(context.Background(), s.Batch)
		manual = append(manual, StepResult{Index: i, Label: s.Label, Kind: StepSubmit,
			BatchID: s.Batch.ID, Seq: s.Batch.Seq, Validation: &res, Err: err})
	}

	if !reflect.DeepEqual(normSteps(rep.Steps), normSteps(manual)) {
		t.Fatalf("replay results must match manual submits")
	}
	if !reflect.DeepEqual(rep.Metrics, e.Metrics()) {
		t.Fatalf("replay metrics must match manual metrics: %+v vs %+v", rep.Metrics, e.Metrics())
	}
	t.Log("回放与手动逐批提交的逐步结论与指标完全一致")
	logMetrics(t, rep.Metrics)
}

// normStep 去除指针地址差异，仅保留结论性内容。
type normStep struct {
	Index               int
	Label, Kind         string
	BatchID             string
	Seq                 uint64
	Version             int
	Accepted, Violation int
	Reasons             []string
	Action              EvolutionAction
	Changes             []Change
	HasErr              bool
}

func normSteps(in []StepResult) []normStep {
	out := make([]normStep, 0, len(in))
	for _, s := range in {
		n := normStep{Index: s.Index, Label: s.Label, Kind: string(s.Kind),
			BatchID: s.BatchID, Seq: s.Seq, HasErr: s.Err != nil}
		if s.Validation != nil {
			n.Version = s.Validation.ContractVersion
			n.Accepted = len(s.Validation.Accepted)
			n.Violation = len(s.Validation.Violations)
			for _, v := range s.Validation.Violations {
				n.Reasons = append(n.Reasons, string(v.Reason)+":"+v.Field+":"+v.Detail)
			}
		}
		if s.Evolution != nil {
			n.Action = s.Evolution.Action
			n.Changes = s.Evolution.Changes
		}
		out = append(out, n)
	}
	return out
}

func normReport(r ReplayReport) any {
	return struct {
		Steps   []normStep
		Metrics Metrics
		Version int
	}{normSteps(r.Steps), r.Metrics, r.ContractVersion}
}
