package engine

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/go-data-quality-monitor-go/internal/contract"
	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
	"github.com/h3yow/go-data-quality-monitor-go/internal/monitor"
	"github.com/h3yow/go-data-quality-monitor-go/internal/validate"
)

func fp(x float64) *float64 { return &x }

func v1() model.Contract {
	return model.Contract{Name: "default", Version: "1.0.0", Fields: []model.FieldSpec{
		{Name: "id", Type: model.TypeInt, Required: true, Nullable: false},
		{Name: "amount", Type: model.TypeFloat, Required: false, Nullable: true, Min: fp(0), Max: fp(10)},
	}, KeyFields: []string{"id"}}
}

func v11Optional() model.Contract {
	c := v1()
	c.Version = "1.1.0"
	c.Fields = append(c.Fields, model.FieldSpec{Name: "tag", Type: model.TypeString, Required: false, Nullable: true})
	return c
}

func v2Breaking() model.Contract {
	c := v1()
	c.Version = "2.0.0"
	c.Fields[0].Type = model.TypeString // id: int -> string，破坏性
	return c
}

func newTestEngine(t *testing.T, p contract.Policy) *Engine {
	t.Helper()
	var clock int64 = 1_000_000
	e := New(Options{
		Policy:   p,
		Monitor:  monitor.Options{AllowedLatenessMs: 1000, FreezeAfter: 1_000_000, FreshnessAlertMs: 0},
		Validate: validate.Options{RuleTimeout: time.Second},
		NowFn:    func() int64 { return clock },
	})
	if err := e.RegisterContract(v1()); err != nil {
		t.Fatal(err)
	}
	return e
}

func batch(id, v string, et int64, recs ...model.Record) model.Batch {
	return model.Batch{BatchID: id, ContractV: v, EventTimeMillis: et, Records: recs}
}

// logResult 打印批次标识、规则版本、关键指标与判定原因（满足单测日志要求）。
func logResult(t *testing.T, r model.BatchResult) {
	t.Helper()
	reasons := []string{}
	for k, n := range r.ReasonCounts {
		reasons = append(reasons, fmt.Sprintf("%s=%d", k, n))
	}
	var wm int64
	var ratio float64
	if r.MetricsAfter != nil {
		wm = r.MetricsAfter.WatermarkMs
		ratio = r.MetricsAfter.CompletenessRatio
	}
	log.Printf("[TEST] batch=%s version=%s outcome=%s total=%d valid=%d invalid=%d watermark=%d completeness=%.3f reasons=%v alerts=%d",
		r.BatchID, r.ContractV, r.Outcome, r.Total, r.ValidCount, r.InvalidCount, wm, ratio, reasons, len(r.Alerts))
}

func TestSubmitAcceptRejectAndReasons(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())

	good := e.Submit(batch("b-good", "1.0.0", 100,
		model.Record{"id": int64(1), "amount": 5.0}))
	logResult(t, good)
	if good.Outcome != model.OutcomeAccepted {
		t.Fatalf("good batch outcome=%s reasons=%v", good.Outcome, good.Reasons)
	}

	bad := e.Submit(batch("b-bad", "1.0.0", 200,
		model.Record{"id": int64(2), "amount": 50.0}, // 越界
		model.Record{"amount": 1.0},                  // 缺 id
		nil,                                          // 畸形
	))
	logResult(t, bad)
	if bad.Outcome != model.OutcomeRejected {
		t.Fatalf("bad batch outcome=%s", bad.Outcome)
	}
	if bad.ReasonCounts[model.ReasonConstraint] != 1 ||
		bad.ReasonCounts[model.ReasonMissingField] != 1 ||
		bad.ReasonCounts[model.ReasonMalformed] != 1 {
		t.Fatalf("reason counts wrong: %+v", bad.ReasonCounts)
	}
	if bad.InvalidCount != 3 || bad.ValidCount != 0 {
		t.Fatalf("counts wrong valid=%d invalid=%d", bad.ValidCount, bad.InvalidCount)
	}
	// 被拒批次不得推进水位。
	if bad.MetricsAfter.WatermarkMs != 100 {
		t.Fatalf("rejected batch advanced watermark to %d", bad.MetricsAfter.WatermarkMs)
	}
}

func TestUnknownAndDeprecatedContract(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())
	unk := e.Submit(batch("b-unk", "9.9.9", 1, model.Record{"id": int64(1)}))
	if unk.Outcome != model.OutcomeRejected ||
		unk.ReasonCounts[model.BatchReasonUnknownContract] != 1 {
		t.Fatalf("unknown contract mishandled: %+v", unk)
	}
	dep := v1()
	dep.Deprecated = true
	dep.Version = "0.9.0"
	if err := e.RegisterContract(dep); err != nil {
		t.Fatal(err)
	}
	r := e.Submit(batch("b-dep", "0.9.0", 1, model.Record{"id": int64(1)}))
	if r.Outcome != model.OutcomeRejected {
		t.Fatalf("deprecated contract accepted: %s", r.Outcome)
	}
}

func TestDuplicateBatchIdempotent(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())
	b := batch("dup", "1.0.0", 100, model.Record{"id": int64(1), "amount": 1.0})
	r1 := e.Submit(b)
	r2 := e.Submit(b)
	r3 := e.Submit(b)
	logResult(t, r2)
	if r2.Outcome != model.OutcomeDuplicate || r3.Outcome != model.OutcomeDuplicate {
		t.Fatalf("duplicate outcomes: %s %s", r2.Outcome, r3.Outcome)
	}
	// 首次结论必须原样返回（计数一致）。
	if r2.ValidCount != r1.ValidCount || r2.InvalidCount != r1.InvalidCount {
		t.Fatal("duplicate verdict differs from first")
	}
	s, _ := e.Snapshot("default")
	if s.TotalAccepted != 1 || s.AcceptedBatches != 1 {
		t.Fatalf("duplicate batch was counted: %+v", s)
	}
}

func TestOutOfOrderDoesNotRegress(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())
	e.Submit(batch("late1", "1.0.0", 1000, model.Record{"id": int64(1), "amount": 1.0}))
	e.Submit(batch("late2", "1.0.0", 2000, model.Record{"id": int64(2), "amount": 2.0}))
	// 严重乱序：超出允许迟到窗口。
	r := e.Submit(batch("late3", "1.0.0", 500, model.Record{"id": int64(3), "amount": 3.0}))
	logResult(t, r)
	if r.MetricsAfter.WatermarkMs != 2000 {
		t.Fatalf("watermark regressed: %d", r.MetricsAfter.WatermarkMs)
	}
	found := false
	for _, a := range r.Alerts {
		if a.Kind == "late_batch" {
			found = true
		}
	}
	if !found {
		t.Fatalf("late batch alert missing: %+v", r.Alerts)
	}
	// 记录仍被计数（不静默丢弃）。
	if r.MetricsAfter.TotalAccepted != 3 {
		t.Fatalf("late record not counted: %+v", r.MetricsAfter)
	}
}

func TestContractEvolutionPolicies(t *testing.T) {
	// 兼容演进：默认 alert，正常接收并升级生效版本。
	e := newTestEngine(t, contract.DefaultPolicy())
	e.Submit(batch("c1", "1.0.0", 100, model.Record{"id": int64(1), "amount": 1.0}))
	if err := e.RegisterContract(v11Optional()); err != nil {
		t.Fatal(err)
	}
	r := e.Submit(batch("c2", "1.1.0", 200,
		model.Record{"id": int64(2), "amount": 2.0, "tag": "x"}))
	logResult(t, r)
	if r.Outcome != model.OutcomeAccepted {
		t.Fatalf("compatible evolution rejected: %s", r.Outcome)
	}
	if !alertKind(r, "contract_compatible_change") {
		t.Fatalf("missing compatible-change alert: %+v", r.Alerts)
	}
	if r.MetricsAfter.ActiveVersion != "1.1.0" {
		t.Fatalf("active version not promoted: %s", r.MetricsAfter.ActiveVersion)
	}

	// 破坏性演进 + reject：拒绝且不升级。
	if err := e.RegisterContract(v2Breaking()); err != nil {
		t.Fatal(err)
	}
	rb := e.Submit(batch("c3", "2.0.0", 300,
		model.Record{"id": "not-an-int", "amount": 1.0}))
	logResult(t, rb)
	if rb.Outcome != model.OutcomeRejected ||
		rb.ReasonCounts[model.BatchReasonContractRejected] != 1 {
		t.Fatalf("breaking change should be rejected: %+v", rb)
	}
	if rb.MetricsAfter.ActiveVersion != "1.1.0" {
		t.Fatal("active version changed on rejection")
	}

	// 破坏性演进 + sideways：旁路计数，不进主基线。
	e2 := newTestEngine(t, contract.Policy{
		OnBreaking: contract.ModeSideways, OnCompatible: contract.ModeAlert, OnDowngrade: contract.ModeAlert,
	})
	if err := e2.RegisterContract(v2Breaking()); err != nil {
		t.Fatal(err)
	}
	e2.Submit(batch("s0", "1.0.0", 100, model.Record{"id": int64(1), "amount": 1.0}))
	rs := e2.Submit(batch("s1", "2.0.0", 200, model.Record{"id": "x", "amount": 2.0}))
	logResult(t, rs)
	if rs.Outcome != model.OutcomeSideways {
		t.Fatalf("sideways outcome=%s", rs.Outcome)
	}
	if rs.MetricsAfter.SidewaysBatches != 1 || rs.MetricsAfter.WatermarkMs != 100 {
		t.Fatalf("sideways polluted mainline: %+v", rs.MetricsAfter)
	}
}

func TestReproducibleVerdict(t *testing.T) {
	mk := func() *Engine { return newTestEngine(t, contract.DefaultPolicy()) }
	b := batch("same", "1.0.0", 100,
		model.Record{"id": int64(1), "amount": 1.0},
		model.Record{"id": "notint"},
		nil,
	)
	r1 := mk().Submit(b)
	r2 := mk().Submit(b)
	if r1.Outcome != r2.Outcome || r1.ValidCount != r2.ValidCount || r1.InvalidCount != r2.InvalidCount {
		t.Fatal("verdict not reproducible across engine instances")
	}
	if len(r1.RecordResults) != len(r2.RecordResults) {
		t.Fatal("record results length differs")
	}
	for i := range r1.RecordResults {
		a, z := r1.RecordResults[i], r2.RecordResults[i]
		if a.Valid != z.Valid || len(a.Violations) != len(z.Violations) {
			t.Fatalf("record %d verdict differs", i)
		}
		for j := range a.Violations {
			if a.Violations[j].Reason != z.Violations[j].Reason {
				t.Fatalf("record %d reason order differs", i)
			}
		}
	}
}

func TestConcurrentSubmitConsistency(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())
	const n = 100
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("g%d-%d", g, i)
				r := e.Submit(batch(id, "1.0.0", int64(g*n+i+1),
					model.Record{"id": int64(g*n + i), "amount": float64(i % 10)}))
				if r.Outcome != model.OutcomeAccepted && r.Outcome != model.OutcomeRejected {
					t.Errorf("unexpected outcome %s for %s", r.Outcome, id)
				}
			}
		}(g)
	}
	wg.Wait()
	s, _ := e.Snapshot("default")
	log.Printf("[TEST] concurrent done accepted_records=%d accepted_batches=%d rejected=%d watermark=%d",
		s.TotalAccepted, s.AcceptedBatches, s.RejectedBatches, s.WatermarkMs)
	if s.TotalAccepted != int64(10*n) {
		t.Fatalf("lost accepted records: %d", s.TotalAccepted)
	}
	if s.AcceptedBatches != int64(10*n) {
		t.Fatalf("lost accepted batches: %d", s.AcceptedBatches)
	}
}

func TestConcurrentDuplicatesCountedOnce(t *testing.T) {
	e := newTestEngine(t, contract.DefaultPolicy())
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				// 所有 goroutine 提交同一个批次 ID，只能有一个被计数。
				e.Submit(batch("the-only-batch", "1.0.0", 100,
					model.Record{"id": int64(1), "amount": 1.0}))
			}
		}()
	}
	wg.Wait()
	s, _ := e.Snapshot("default")
	if s.TotalAccepted != 1 || s.AcceptedBatches != 1 {
		t.Fatalf("duplicate counted under concurrency: %+v", s)
	}
}

func TestPanicRuleDoesNotBreakEngine(t *testing.T) {
	e := New(Options{
		Policy:  contract.DefaultPolicy(),
		Monitor: monitor.Options{AllowedLatenessMs: 1000, FreezeAfter: 1_000_000, FreshnessAlertMs: 0},
		Validate: validate.Options{
			RuleTimeout: 100 * time.Millisecond,
			CustomRules: []validate.CustomRule{
				func(rec model.Record) (model.Reason, string) { panic("boom") },
			},
		},
	})
	if err := e.RegisterContract(v1()); err != nil {
		t.Fatal(err)
	}
	r1 := e.Submit(batch("p1", "1.0.0", 100, model.Record{"id": int64(1), "amount": 1.0}))
	logResult(t, r1)
	if r1.Outcome != model.OutcomeRejected ||
		r1.ReasonCounts[model.ReasonCustomPanic] != 1 {
		t.Fatalf("panic rule not isolated: %+v", r1)
	}
	// 引擎仍然健康，后续批次正常处理。
	r2 := e.Submit(batch("p2", "1.0.0", 200, model.Record{"id": int64(2), "amount": 2.0}))
	if r2.Outcome != model.OutcomeRejected {
		// 规则仍然存在，所以也会 panic；重点是引擎不崩、不泄漏。
	}
	if r2.ReasonCounts[model.ReasonCustomPanic] != 1 {
		t.Fatalf("engine state corrupted after panic: %+v", r2)
	}
}

func alertKind(r model.BatchResult, kind string) bool {
	for _, a := range r.Alerts {
		if a.Kind == kind {
			return true
		}
	}
	return false
}
