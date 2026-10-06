package dq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentSubmitAndEvolve 并发提交批次与推进契约版本交错进行：
//   - 每批按提交瞬间的契约版本判定，版本号只可能来自 [1, maxVersion]；
//   - 兼容演进全部生效，不兼容演进按策略处理，绝不出现半新半旧契约；
//   - 指标一致性：唯一批次数、水位、接受+违规总数与记录总数对得上；
//   - race 检测下稳定通过。
func TestConcurrentSubmitAndEvolve(t *testing.T) {
	const (
		nBatches   = 40
		nEvolutions = 8 // v2..v9 兼容演进(每次新增一个可选字段)
		nSubmitters = 4
	)
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

	newEngine := func() *Engine {
		return NewEngine(replayContract(), WithCompatMode(CompatReject),
			WithClock(func() time.Time { return base.Add(time.Hour) }))
	}

	// evolutions 预生成 v2..v9 的兼容演进(累积新增可选字段)。
	evolutions := make([]Contract, 0, nEvolutions)
	for v := 2; v <= nEvolutions+1; v++ {
		c := replayContract()
		c.Version = v
		for pv := 2; pv <= v; pv++ {
			c.Fields = append(c.Fields, FieldSpec{
				Name: fmt.Sprintf("opt_%d", pv), Type: TypeString, Required: false,
			})
		}
		evolutions = append(evolutions, c)
	}

	run := func() (*Engine, []ValidationResult) {
		e := newEngine()
		results := make([]ValidationResult, nBatches)
		var wg sync.WaitGroup
		// 演进协程：与提交交错推进版本。
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, c := range evolutions {
				if err := e.Evolve(c); err != nil {
					t.Errorf("compatible evolve to v%d rejected: %v", c.Version, err)
				}
			}
		}()
		// 提交协程：分片并发提交全部批次。
		var mu sync.Mutex
		for w := 0; w < nSubmitters; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for i := worker; i < nBatches; i += nSubmitters {
					b := Batch{
						ID:        fmt.Sprintf("cb%d", i+1),
						Seq:       uint64(i + 1),
						EventTime: base.Add(time.Duration(i) * time.Minute),
						Records: []Record{
							{"id": i*2 + 1, "amount": float64(i)},
							{"id": i*2 + 2, "amount": float64(i + 1)},
						},
					}
					res, err := e.Submit(context.Background(), b)
					if err != nil {
						t.Errorf("submit %s: %v", b.ID, err)
						return
					}
					mu.Lock()
					results[i] = res
					mu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		return e, results
	}

	e, results := run()

	// 每批判定版本必须在合法范围内，且记录级判定与版本一致。
	for i, res := range results {
		if res.ContractVersion < 1 || res.ContractVersion > nEvolutions+1 {
			t.Fatalf("batch cb%d judged by impossible contract v%d", i+1, res.ContractVersion)
		}
		if len(res.Accepted)+len(res.Violations) != 2 {
			t.Fatalf("batch cb%d: accepted+violations=%d, want 2", i+1, len(res.Accepted)+len(res.Violations))
		}
	}

	// 全部兼容演进生效，最终契约为最高版本。
	if got := e.Contract().Version; got != nEvolutions+1 {
		t.Fatalf("final contract version %d, want %d", got, nEvolutions+1)
	}

	// 指标一致性：无重复、无丢失、水位连续。
	m := e.Metrics()
	if m.SeenBatches != nBatches {
		t.Fatalf("seen batches %d, want %d", m.SeenBatches, nBatches)
	}
	if m.Watermark != nBatches {
		t.Fatalf("watermark %d, want %d (all seqs 1..%d submitted)", m.Watermark, nBatches, nBatches)
	}
	if m.TotalAccepted+m.TotalViolations != nBatches*2 {
		t.Fatalf("accepted+violations=%d, want %d", m.TotalAccepted+m.TotalViolations, nBatches*2)
	}
	t.Logf("concurrent run: watermark=%d seen=%d accepted=%d violations=%d finalContract=v%d",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations, e.Contract().Version)
	for i, res := range results {
		t.Logf("  batch cb%d contract=v%d accepted=%d violations=%d",
			i+1, res.ContractVersion, len(res.Accepted), len(res.Violations))
	}
}

// TestConcurrentEvolveRejectedKeepsConsistency 并发提交与不兼容演进交错：
// 演进被拒绝后契约保持不变，后续批次仍按原版本判定，指标不出现错乱。
func TestConcurrentEvolveRejectedKeepsConsistency(t *testing.T) {
	const nBatches = 30
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	e := NewEngine(replayContract(), WithCompatMode(CompatReject),
		WithClock(func() time.Time { return base.Add(time.Hour) }))

	bad := replayContract()
	bad.Version = 2
	bad.Fields[1].Required = false // 占位，下面真正收紧
	bad.Fields[1].Constraint = &Constraint{Min: fptr(1000)} // 收紧下界 -> 不兼容

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := e.Evolve(bad); err == nil {
				t.Error("incompatible evolve unexpectedly accepted")
			}
		}
	}()
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < nBatches; i += 3 {
				b := Batch{
					ID:        fmt.Sprintf("rb%d", i+1),
					Seq:       uint64(i + 1),
					EventTime: base.Add(time.Duration(i) * time.Minute),
					Records:   []Record{{"id": i + 1, "amount": float64(i)}},
				}
				res, err := e.Submit(context.Background(), b)
				if err != nil {
					t.Errorf("submit %s: %v", b.ID, err)
					return
				}
				if res.ContractVersion != 1 {
					t.Errorf("batch %s judged by v%d after rejected evolution, want v1", b.ID, res.ContractVersion)
				}
			}
		}(w)
	}
	wg.Wait()

	if got := e.Contract().Version; got != 1 {
		t.Fatalf("contract moved to v%d despite rejection", got)
	}
	m := e.Metrics()
	if m.SeenBatches != nBatches || m.Watermark != nBatches {
		t.Fatalf("metrics inconsistent: seen=%d watermark=%d, want %d", m.SeenBatches, m.Watermark, nBatches)
	}
	t.Logf("after rejected evolutions: contract=v%d watermark=%d seen=%d accepted=%d violations=%d",
		e.Contract().Version, m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations)
}

// TestConcurrentDuplicateSubmit 并发重复提交同一批：监控只计入一次。
func TestConcurrentDuplicateSubmit(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	e := NewEngine(replayContract(), WithClock(func() time.Time { return base.Add(time.Hour) }))
	b := Batch{ID: "dup", Seq: 1, EventTime: base, Records: []Record{{"id": 1, "amount": 1.0}}}

	const n = 16
	results := make([]ValidationResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := e.Submit(context.Background(), b)
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			results[i] = res
		}(i)
	}
	wg.Wait()

	// 每次提交结论一致(可复现)，监控只计入一次。
	for i, res := range results {
		if len(res.Accepted) != 1 || len(res.Violations) != 0 {
			t.Fatalf("submission %d: accepted=%d violations=%d, want 1/0", i, len(res.Accepted), len(res.Violations))
		}
	}
	m := e.Metrics()
	if m.SeenBatches != 1 || m.TotalAccepted != 1 {
		t.Fatalf("duplicate submit counted %d batches / %d accepted, want 1/1", m.SeenBatches, m.TotalAccepted)
	}
	t.Logf("concurrent duplicate submit x%d: seen=%d accepted=%d watermark=%d",
		n, m.SeenBatches, m.TotalAccepted, m.Watermark)
}
