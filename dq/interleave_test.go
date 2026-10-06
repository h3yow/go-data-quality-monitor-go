package dq

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func ilContract(version int) Contract {
	return Contract{
		Version: version,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "amount", Type: TypeFloat, Required: true},
			{Name: "note", Type: TypeString, Required: false},
		},
		KeyGroup: []string{"id"},
	}
}

// TestInterleaveConcurrentSubmitAndEvolve 并发提交批次，同时并发地：
//   - 推进一次兼容演进 v2 -> v3(note 从可选变必填的尝试会被拒绝，见下)；
//
// 本用例用 Reject 策略：收紧演进必须被拒绝，所有批次始终按 v2 判定，
// 指标不出现半新半旧状态。
func TestInterleaveConcurrentSubmitAndEvolve(t *testing.T) {
	const workers = 8
	const perWorker = 25

	run := func(t *testing.T) (int, int, map[string]int) {
		e := NewEngine(ilContract(2), WithCompatMode(CompatReject))
		base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)

		// 收紧契约：note 可选 -> 必填(应被拒绝)。
		tight := ilContract(3)
		tight.Fields[2].Required = true

		versions := make(map[string]int, workers*perWorker)
		var vmu sync.Mutex
		var wg sync.WaitGroup

		// 演进者：在整个提交窗口内反复尝试收紧演进，必须始终被拒绝。
		stop := make(chan struct{})
		eg := sync.WaitGroup{}
		eg.Add(1)
		go func() {
			defer eg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if rep := e.EvolveDetailed(tight); rep.Action != EvolutionRejected {
						t.Errorf("tightening during concurrent submit must be rejected, got action=%s", rep.Action)
						return
					}
				}
			}
		}()

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					seq := uint64(w*perWorker + i + 1)
					b := Batch{
						ID: fmt.Sprintf("c-w%d-%d", w, i), Seq: seq, EventTime: base,
						Records: []Record{
							{"id": int(seq), "amount": float64(seq)},
							{"id": int(seq) + 100000}, // amount 缺失, 任何版本都违规
						},
					}
					res, err := e.Submit(context.Background(), b)
					if err != nil {
						t.Errorf("submit %s: %v", b.ID, err)
						return
					}
					vmu.Lock()
					versions[b.ID] = res.ContractVersion
					vmu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		close(stop)
		eg.Wait()

		m := e.Metrics()
		vmu.Lock()
		defer vmu.Unlock()
		if e.Contract().Version != 2 {
			t.Fatalf("rejected tightening must keep v2, got v%d", e.Contract().Version)
		}
		for id, v := range versions {
			if v != 2 {
				t.Fatalf("batch %s judged by v%d during rejected evolution window (want v2)", id, v)
			}
		}
		total := workers * perWorker
		if m.SeenBatches != total {
			t.Fatalf("seen want %d, got %d", total, m.SeenBatches)
		}
		return m.TotalAccepted, m.TotalViolations, versions
	}

	accepted, violations, _ := run(t)
	total := workers * perWorker
	if accepted != total || violations != total {
		t.Fatalf("metrics inconsistent under interleaving: accepted=%d violations=%d (want %d/%d)",
			accepted, violations, total, total)
	}
	t.Logf("交错并发(收紧演进持续被拒): seen=%d accepted=%d violations=%d 全部批次 rule=v2",
		total, accepted, violations)
}

// TestInterleaveWarnAppliedDuringConcurrency 并发窗口内一次兼容收紧在
// Warn 策略下生效：生效后的批次必须按 v3 判定，且缺 note 的记录开始违规；
// 版本边界两侧的批次各自归属清晰，无"半新半旧"。
func TestInterleaveWarnAppliedDuringConcurrency(t *testing.T) {
	e := NewEngine(ilContract(2), WithCompatMode(CompatWarn))

	// v3：note 必填(收紧), Warn 下强制应用。
	v3 := ilContract(3)
	v3.Fields[2].Required = true

	var wg sync.WaitGroup
	verSeen := make(map[int]int)
	var mu sync.Mutex

	// 先并发提交一批 v2 批次。
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := e.Submit(context.Background(), Batch{
				ID: fmt.Sprintf("pre-%d", i), Seq: uint64(i + 1),
				EventTime: time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC),
				Records:   []Record{{"id": i, "amount": 1.0}}, // note 缺失在 v2 合法
			})
			if err != nil {
				t.Errorf("pre submit: %v", err)
				return
			}
			mu.Lock()
			verSeen[res.ContractVersion]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	rep := e.EvolveDetailed(v3)
	if rep.Action != EvolutionApplied {
		t.Fatalf("warn mode must apply tightening, got %s", rep.Action)
	}
	t.Logf("演进生效:\n%s", rep)

	// 生效后并发提交：同一形态的记录现在因缺 note 违规，且规则版本为 v3。
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := e.Submit(context.Background(), Batch{
				ID: fmt.Sprintf("post-%d", i), Seq: uint64(100 + i),
				EventTime: time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC),
				Records:   []Record{{"id": i, "amount": 1.0}}, // note 缺失: v3 下 field_missing
			})
			if err != nil {
				t.Errorf("post submit: %v", err)
				return
			}
			if res.ContractVersion != 3 {
				t.Errorf("post-evolution batch must use v3, got v%d", res.ContractVersion)
			}
			if len(res.Violations) != 1 || res.Violations[0].Field != "note" {
				t.Errorf("post-evolution: want note missing, got %+v", res.Violations)
			}
			mu.Lock()
			verSeen[res.ContractVersion]++
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	m := e.Metrics()
	t.Logf("版本归属: %+v; 指标 accepted=%d violations=%d", verSeen, m.TotalAccepted, m.TotalViolations)
	if verSeen[2] != 20 || verSeen[3] != 20 {
		t.Fatalf("want exactly 20 batches judged by v2 and 20 by v3, got %+v", verSeen)
	}
	// v2 批次 20 接受；v3 批次 20 全违规。
	if m.TotalAccepted != 20 || m.TotalViolations != 20 {
		t.Fatalf("metrics straddling version boundary inconsistent: %+v", m)
	}
}

// TestInterleaveReplayScripted 脚本化交错：兼容演进生效、收紧被旁路后，
// 后续批次不受影响继续按旧版本判定；重复执行结论一致。
func TestInterleaveScriptedBypass(t *testing.T) {
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)
	v2 := ilContract(2)
	// v3 兼容放宽：amount 必填 -> 可选。
	v3 := ilContract(3)
	v3.Fields[1].Required = false
	// v4 收紧：note 可选 -> 必填(旁路)。
	v4 := ilContract(4)
	v4.Fields[2].Required = true

	steps := []Step{
		{Kind: StepSubmit, Label: "v2: amount 必填", Batch: Batch{
			ID: "s1", Seq: 1, EventTime: base, Records: []Record{{"id": 1, "note": "x"}}}},
		{Kind: StepEvolve, Label: "放宽到 v3", Contract: v3},
		{Kind: StepSubmit, Label: "v3: amount 可缺, 接受", Batch: Batch{
			ID: "s2", Seq: 2, EventTime: base, Records: []Record{{"id": 2, "note": "y"}}}},
		{Kind: StepEvolve, Label: "收紧到 v4 (旁路)", Contract: v4},
		{Kind: StepSubmit, Label: "旁路后仍按 v3: note 可缺 amount 可缺, 接受", Batch: Batch{
			ID: "s3", Seq: 3, EventTime: base, Records: []Record{{"id": 3}}}},
	}

	fixedNow := base.Add(10 * time.Minute)
	run := func() ReplayReport {
		r := NewReplayer(v2, WithCompatMode(CompatBypass),
			WithClock(func() time.Time { return fixedNow }))
		return r.Run(context.Background(), steps)
	}
	rep := run()
	for _, s := range rep.Steps {
		logStep(t, s)
	}
	logMetrics(t, rep.Metrics)

	// s1 在 v2 下缺 amount -> 1 违规；s2/s3 在 v3 下接受；v4 被旁路, 最终版本仍 v3。
	wantVersion := map[string]int{"s1": 2, "s2": 3, "s3": 3}
	for _, s := range rep.Steps {
		if s.Kind != StepSubmit {
			continue
		}
		if got := s.Validation.ContractVersion; got != wantVersion[s.BatchID] {
			t.Fatalf("batch %s want v%d got v%d", s.BatchID, wantVersion[s.BatchID], got)
		}
	}
	if rep.ContractVersion != 3 {
		t.Fatalf("bypassed evolution must keep v3, got v%d", rep.ContractVersion)
	}
	if rep.Metrics.TotalAccepted != 2 || rep.Metrics.TotalViolations != 1 {
		t.Fatalf("want 2 accepted 1 violation, got %d/%d",
			rep.Metrics.TotalAccepted, rep.Metrics.TotalViolations)
	}
	if len(rep.Warnings) != 1 {
		t.Fatalf("bypass must record one warning, got %v", rep.Warnings)
	}

	// 同一交错脚本重复跑结论一致。
	again := run()
	if !reportsEqual(rep, again) {
		t.Fatal("interleaved replay must be reproducible")
	}
	t.Log("交错脚本重复回放结论一致")
}

func reportsEqual(a, b ReplayReport) bool {
	return reflect.DeepEqual(normReport(a), normReport(b)) // 复用 replay_test.go 的规范化帮助器
}
