package monitor

import (
	"strings"
	"sync"
	"testing"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

func rec(id int64, v float64) model.Record { return model.Record{"id": id, "v": v} }

func validResults(n int) []model.RecordResult {
	out := make([]model.RecordResult, n)
	for i := range out {
		out[i] = model.RecordResult{Index: i, Valid: true}
	}
	return out
}

func apply(m Monitor, bid string, et int64, records []model.Record, valid, invalid int) ApplyResult {
	return m.Apply(ApplyRequest{
		Batch:         model.Batch{BatchID: bid, EventTimeMillis: et, Records: records},
		ContractName:  "ds",
		ValidCount:    valid,
		InvalidCount:  invalid,
		RecordResults: mixedResults(len(records), invalid),
		SeenTimeMs:    et + 5,
	})
}

func mixedResults(n, invalid int) []model.RecordResult {
	out := make([]model.RecordResult, n)
	for i := range out {
		out[i] = model.RecordResult{Index: i, Valid: i >= invalid}
	}
	return out
}

func TestWatermarkMonotonicOutOfOrder(t *testing.T) {
	var clock int64 = 1000
	m := New("ds", Options{AllowedLatenessMs: 10, FreezeAfter: 1000, FreshnessAlertMs: 0}, func() int64 { return clock })

	r1 := apply(m, "b1", 100, []model.Record{rec(1, 1)}, 1, 0)
	if r1.Snapshot.WatermarkMs != 100 {
		t.Fatalf("watermark=%d want 100", r1.Snapshot.WatermarkMs)
	}
	// 乱序但在允许窗口内：水位保持 100，不重复/倒退。
	r2 := apply(m, "b2", 95, []model.Record{rec(2, 2)}, 1, 0)
	if r2.Snapshot.WatermarkMs != 100 {
		t.Fatalf("out-of-order moved watermark to %d", r2.Snapshot.WatermarkMs)
	}
	// 过晚批次：产生 late_batch 告警，水位不动。
	r3 := apply(m, "b3", 50, []model.Record{rec(3, 3)}, 1, 0)
	if r3.Snapshot.WatermarkMs != 100 {
		t.Fatalf("late batch moved watermark to %d", r3.Snapshot.WatermarkMs)
	}
	if !hasAlert(r3.Alerts, "late_batch") {
		t.Fatalf("missing late_batch alert: %+v", r3.Alerts)
	}
	// 被拒批次不推进水位。
	r4 := apply(m, "b4", 200, []model.Record{rec(4, 4), rec(5, 5)}, 1, 1)
	if r4.Snapshot.WatermarkMs != 100 {
		t.Fatalf("rejected batch moved watermark to %d", r4.Snapshot.WatermarkMs)
	}
	if r4.Outcome != model.OutcomeRejected {
		t.Fatalf("outcome=%s want rejected", r4.Outcome)
	}
	// 合法的更新批次推进水位。
	r5 := apply(m, "b5", 300, []model.Record{rec(6, 6)}, 1, 0)
	if r5.Snapshot.WatermarkMs != 300 {
		t.Fatalf("watermark=%d want 300", r5.Snapshot.WatermarkMs)
	}
	// 记录级计数始终准确（迟到/被拒都不静默丢弃）。
	snap := m.Snapshot()
	if snap.TotalAccepted != 5 || snap.TotalRejected != 1 {
		t.Fatalf("counts wrong: %+v", snap)
	}
	if snap.AcceptedBatches != 4 || snap.RejectedBatches != 1 {
		t.Fatalf("batch counts wrong: %+v", snap)
	}
}

func TestBaselineFreezeAndDrift(t *testing.T) {
	var clock int64 = 10
	opts := Options{AllowedLatenessMs: 0, FreezeAfter: 10, DriftZ: 2.0, FreshnessAlertMs: 0}
	m := New("ds", opts, func() int64 { return clock })

	// 10 个稳定值冻结基线：mean=5, std≈3.03
	recs := make([]model.Record, 0, 10)
	for i := int64(1); i <= 10; i++ {
		recs = append(recs, rec(i, float64(i)))
	}
	r := apply(m, "freeze", 100, recs, 10, 0)
	d := r.Snapshot.Distributions["v"]
	if d.Count != 10 || d.FrozenAt == 0 {
		t.Fatalf("baseline not frozen: %+v", d)
	}
	if d.Mean != 5.5 {
		t.Fatalf("mean=%v want 5.5", d.Mean)
	}

	// 正常批次不应漂移告警。
	normal := []model.Record{rec(11, 5.8), rec(12, 5.1)}
	r2 := apply(m, "normal", 200, normal, 2, 0)
	if hasAlert(r2.Alerts, "distribution_drift") {
		t.Fatalf("unexpected drift alert: %+v", r2.Alerts)
	}
	// 冻结后基线不再变化。
	d2 := r2.Snapshot.Distributions["v"]
	if d2.Count != 10 || d2.Mean != d.Mean {
		t.Fatalf("baseline changed after freeze: %+v vs %+v", d, d2)
	}

	// 极端均值批次触发漂移。
	drift := []model.Record{rec(13, 500), rec(14, 510)}
	r3 := apply(m, "drift", 300, drift, 2, 0)
	if !hasAlert(r3.Alerts, "distribution_drift") {
		t.Fatalf("missing drift alert: %+v", r3.Alerts)
	}
}

func TestSidewaysBypassesBaseline(t *testing.T) {
	var clock int64
	m := New("ds", DefaultOptions(), func() int64 { return clock })
	res := m.Apply(ApplyRequest{
		Batch:      model.Batch{BatchID: "s1", EventTimeMillis: 999, Records: []model.Record{rec(1, 9)}},
		ValidCount: 1,
		Sideways:   true,
		SeenTimeMs: 1,
	})
	if res.Outcome != model.OutcomeSideways {
		t.Fatalf("outcome=%s", res.Outcome)
	}
	s := m.Snapshot()
	if s.SidewaysBatches != 1 || s.WatermarkMs != 0 || s.TotalAccepted != 0 {
		t.Fatalf("sideways polluted state: %+v", s)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	m := New("ds", Options{FreezeAfter: 1000, FreshnessAlertMs: 0}, nil)
	apply(m, "b1", 100, []model.Record{rec(1, 1)}, 1, 0)
	s1 := m.Snapshot()
	s1.Distributions["v"] = model.FieldDist{Field: "v", Count: 999}
	s2 := m.Snapshot()
	if s2.Distributions["v"].Count == 999 {
		t.Fatal("snapshot shares mutable state with monitor")
	}
}

func TestConcurrentApplySnapshotConsistency(t *testing.T) {
	var clock int64 = 1000
	m := New("ds", Options{AllowedLatenessMs: 1 << 40, FreezeAfter: 1_000_000, FreshnessAlertMs: 0},
		func() int64 { return clock })
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				bid := strings.Repeat("x", 0) + "g" + itoa(g) + "-" + itoa(i)
				et := int64(g*1000 + i)
				r := apply(m, bid, et, []model.Record{rec(int64(g*100+i), float64(i))}, 1, 0)
				s := r.Snapshot
				if s == nil || s.WatermarkMs < 0 {
					t.Errorf("bad snapshot")
				}
			}
		}(g)
	}
	wg.Wait()
	s := m.Snapshot()
	if s.AcceptedBatches != 400 || s.TotalAccepted != 400 {
		t.Fatalf("lost updates under concurrency: %+v", s)
	}
	want := int64(7*1000 + 49)
	if s.WatermarkMs != want {
		t.Fatalf("watermark=%d want %d", s.WatermarkMs, want)
	}
}

func hasAlert(alerts []model.Alert, kind string) bool {
	for _, a := range alerts {
		if a.Kind == kind {
			return true
		}
	}
	return false
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
