package dq

import (
	"context"
	"testing"
	"time"
)

// replayContract 回放测试用契约。
func replayContract() Contract {
	return Contract{
		Version: 1,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "amount", Type: TypeFloat, Required: true, Constraint: &Constraint{Min: fptr(0)}},
		},
		KeyGroup: []string{"id"},
	}
}

// replayBatches 预置 6 批数据，含合法与违规记录。
func replayBatches() []Batch {
	base := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	rec := func(id int, amount float64) Record { return Record{"id": id, "amount": amount} }
	return []Batch{
		{ID: "b1", Seq: 1, EventTime: base, Records: []Record{rec(1, 10), rec(2, 20)}},
		{ID: "b2", Seq: 2, EventTime: base.Add(time.Minute), Records: []Record{rec(3, 30), rec(3, 31)}}, // 批内键重复
		{ID: "b3", Seq: 3, EventTime: base.Add(2 * time.Minute), Records: []Record{rec(4, -1)}},           // 约束越界
		{ID: "b4", Seq: 4, EventTime: base.Add(3 * time.Minute), Records: []Record{rec(5, 50), {"id": 6}}}, // 缺字段
		{ID: "b5", Seq: 5, EventTime: base.Add(4 * time.Minute), Records: []Record{rec(7, 70)}},
		{ID: "b6", Seq: 6, EventTime: base.Add(5 * time.Minute), Records: []Record{rec(8, 80), rec(9, 90)}},
	}
}

func logReport(t *testing.T, title string, rep ReplayReport) {
	t.Helper()
	t.Logf("== %s ==", title)
	for _, line := range rep.Timeline {
		t.Logf("  %s", line)
	}
	for _, o := range rep.Outcomes {
		for _, v := range o.Violations {
			t.Logf("    violation batch=%s row=%d reason=%s field=%s %s",
				o.BatchID, v.Row, v.Reason, v.Field, v.Detail)
		}
	}
	m := rep.Metrics
	t.Logf("  metrics: watermark=%d seen=%d accepted=%d violations=%d",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations)
}

// 关键指标：乱序/重复/迟到场景下必须与正序一致的部分。
type keyMetrics struct {
	watermark  uint64
	seen       int
	accepted   int
	violations int
}

func keyOfMetrics(m Metrics) keyMetrics {
	return keyMetrics{m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations}
}

func TestReplayInOrderMatchesManual(t *testing.T) {
	batches := replayBatches()

	rep := NewReplayer(replayContract()).RunBatches(batches)
	logReport(t, "正序回放", rep)

	// 手动逐批提交作为基准。
	e := NewEngine(replayContract())
	var manual []ValidationResult
	for _, b := range batches {
		res, err := e.Submit(context.Background(), b)
		if err != nil {
			t.Fatalf("manual submit %s: %v", b.ID, err)
		}
		manual = append(manual, res)
	}

	if len(rep.Outcomes) != len(manual) {
		t.Fatalf("outcome count %d != manual %d", len(rep.Outcomes), len(manual))
	}
	for i, o := range rep.Outcomes {
		if o.Accepted != len(manual[i].Accepted) || len(o.Violations) != len(manual[i].Violations) {
			t.Fatalf("batch %s mismatch: replay accepted=%d violations=%d, manual accepted=%d violations=%d",
				o.BatchID, o.Accepted, len(o.Violations), len(manual[i].Accepted), len(manual[i].Violations))
		}
		if o.ContractVersion != manual[i].ContractVersion {
			t.Fatalf("batch %s contract version %d != manual %d", o.BatchID, o.ContractVersion, manual[i].ContractVersion)
		}
	}
	if got, want := keyOfMetrics(rep.Metrics), keyOfMetrics(e.Metrics()); got != want {
		t.Fatalf("metrics mismatch: replay %+v, manual %+v", got, want)
	}
}

func TestReplayScrambledDuplicateLate(t *testing.T) {
	batches := replayBatches()

	inOrder := NewReplayer(replayContract()).RunBatches(batches)
	logReport(t, "正序回放", inOrder)

	// 乱序 + b2 迟到 + b1/b3 重复投递。
	scrambled := DeferLast(Permute(batches, 42), "b2")
	scrambled = Redeliver(scrambled, "b1", "b3")
	rep := NewReplayer(replayContract()).RunBatches(scrambled)
	logReport(t, "乱序+重复+迟到回放", rep)

	// 同一脚本重复回放，结论必须一致。
	again := NewReplayer(replayContract()).RunBatches(scrambled)
	if got, want := keyOfMetrics(again.Metrics), keyOfMetrics(rep.Metrics); got != want {
		t.Fatalf("same script replayed twice diverged: %+v vs %+v", got, want)
	}

	// 关键指标与正序一致。
	if got, want := keyOfMetrics(rep.Metrics), keyOfMetrics(inOrder.Metrics); got != want {
		t.Fatalf("scrambled metrics %+v != in-order %+v", got, want)
	}

	// 重复投递被标记且幂等：b1/b3 各出现两次，第二次为 duplicate。
	seen := map[string]int{}
	dup := map[string]int{}
	for _, o := range rep.Outcomes {
		seen[o.BatchID]++
		if o.Duplicate {
			dup[o.BatchID]++
		}
	}
	for _, id := range []string{"b1", "b3"} {
		if seen[id] != 2 || dup[id] != 1 {
			t.Fatalf("batch %s: seen=%d dup=%d, want seen=2 dup=1", id, seen[id], dup[id])
		}
	}

	// 每个唯一批次无论到达顺序，判定结论一致(契约未变)。
	want := map[string]BatchOutcome{}
	for _, o := range inOrder.Outcomes {
		want[o.BatchID] = o
	}
	for _, o := range rep.Outcomes {
		w := want[o.BatchID]
		if o.Accepted != w.Accepted || len(o.Violations) != len(w.Violations) {
			t.Fatalf("batch %s outcome differs under scrambling: %+v vs %+v", o.BatchID, o, w)
		}
	}
}

func TestReplayInterleavedEvolution(t *testing.T) {
	batches := replayBatches()

	// v2 兼容：amount 下界放宽；v3 不兼容：amount 变可选? 不——收紧：新增必需字段。
	v2 := replayContract()
	v2.Version = 2
	v2.Fields[1].Constraint = &Constraint{Min: fptr(-100)} // 放宽下界
	v3 := replayContract()
	v3.Version = 3
	v3.Fields = append(v3.Fields, FieldSpec{Name: "must", Type: TypeString, Required: true}) // 新增必需字段

	modes := []struct {
		name    string
		mode    CompatMode
		applied bool // v3 是否生效
	}{
		{"拒绝", CompatReject, false},
		{"旁路", CompatBypass, false},
		{"告警", CompatWarn, true},
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReplayer(replayContract(), WithCompatMode(tc.mode))
			rep := r.Run([]ReplayEvent{
				SubmitEvent(batches[0]),
				EvolveEvent(v2),       // 兼容，生效
				SubmitEvent(batches[1]), // 按 v2
				EvolveEvent(v3),       // 不兼容，按策略处理
				SubmitEvent(batches[2]), // 按当时生效版本
				SubmitEvent(batches[3]),
			})
			logReport(t, "交错演进("+tc.name+")", rep)

			if len(rep.Evolutions) != 2 {
				t.Fatalf("expected 2 evolutions, got %d", len(rep.Evolutions))
			}
			if !rep.Evolutions[0].Applied {
				t.Fatalf("compatible v2 must apply: %+v", rep.Evolutions[0])
			}
			if rep.Evolutions[1].Applied != tc.applied {
				t.Fatalf("v3 applied=%v, want %v (mode %v)", rep.Evolutions[1].Applied, tc.applied, tc.mode)
			}
			if rep.Evolutions[1].Report.Compatible() {
				t.Fatal("v3 report must flag incompatibility")
			}

			// 每批按提交瞬间的版本判定。
			wantVersions := []int{1, 2, 2, 2}
			if tc.applied {
				wantVersions = []int{1, 2, 3, 3}
			}
			for i, o := range rep.Outcomes {
				if o.ContractVersion != wantVersions[i] {
					t.Fatalf("batch %s judged by v%d, want v%d", o.BatchID, o.ContractVersion, wantVersions[i])
				}
			}

			// 同一脚本重复回放结论一致。
			r2 := NewReplayer(replayContract(), WithCompatMode(tc.mode))
			rep2 := r2.Run([]ReplayEvent{
				SubmitEvent(batches[0]), EvolveEvent(v2), SubmitEvent(batches[1]),
				EvolveEvent(v3), SubmitEvent(batches[2]), SubmitEvent(batches[3]),
			})
			if keyOfMetrics(rep2.Metrics) != keyOfMetrics(rep.Metrics) {
				t.Fatalf("repeated replay diverged: %+v vs %+v",
					keyOfMetrics(rep2.Metrics), keyOfMetrics(rep.Metrics))
			}
			for i := range rep.Outcomes {
				if rep2.Outcomes[i].ContractVersion != rep.Outcomes[i].ContractVersion {
					t.Fatalf("repeated replay: batch %s version %d != %d",
						rep.Outcomes[i].BatchID, rep2.Outcomes[i].ContractVersion, rep.Outcomes[i].ContractVersion)
				}
			}
		})
	}
}
