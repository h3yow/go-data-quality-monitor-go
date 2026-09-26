package dq

import (
	"fmt"
	"testing"
	"time"
)

func monContract() Contract {
	return Contract{
		Version: 1,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "amount", Type: TypeFloat, Required: true},
		},
		KeyGroup: []string{"id"},
	}
}

func observeBatch(m *Monitor, id string, seq uint64, eventTime time.Time, recs ...Record) {
	c := monContract()
	res := NewValidator(c, nil, 0).Validate(Batch{ID: id, Seq: seq, Records: recs, EventTime: eventTime})
	m.Observe(Batch{ID: id, Seq: seq, Records: recs, EventTime: eventTime}, res, c)
}

func TestMonitorWatermarkOutOfOrder(t *testing.T) {
	m := NewMonitor()
	now := time.Now()
	// 乱序提交：2, 3, 1
	observeBatch(m, "b2", 2, now, Record{"id": 2, "amount": 10.0})
	if wm := m.Snapshot().Watermark; wm != 0 {
		t.Fatalf("watermark must not advance with gap, got %d", wm)
	}
	observeBatch(m, "b3", 3, now, Record{"id": 3, "amount": 10.0})
	if wm := m.Snapshot().Watermark; wm != 0 {
		t.Fatalf("watermark must not advance with gap, got %d", wm)
	}
	observeBatch(m, "b1", 1, now, Record{"id": 1, "amount": 10.0})
	snap := m.Snapshot()
	t.Logf("watermark=%d seen=%d accepted=%d", snap.Watermark, snap.SeenBatches, snap.TotalAccepted)
	if snap.Watermark != 3 {
		t.Fatalf("watermark must catch up to 3 after gap filled, got %d", snap.Watermark)
	}
	if snap.TotalAccepted != 3 {
		t.Fatalf("want 3 accepted, got %d", snap.TotalAccepted)
	}
}

func TestMonitorDuplicateBatchIdempotent(t *testing.T) {
	m := NewMonitor()
	now := time.Now()
	recs := []Record{{"id": 1, "amount": 5.0}, {"id": 2, "amount": 6.0}}
	observeBatch(m, "b1", 1, now, recs...)
	observeBatch(m, "b1", 1, now, recs...) // 重复提交
	observeBatch(m, "b1", 1, now, recs...) // 再次重复
	snap := m.Snapshot()
	t.Logf("watermark=%d seen=%d accepted=%d", snap.Watermark, snap.SeenBatches, snap.TotalAccepted)
	if snap.TotalAccepted != 2 {
		t.Fatalf("duplicate batch must not double count, got accepted=%d", snap.TotalAccepted)
	}
	if snap.SeenBatches != 1 {
		t.Fatalf("duplicate batch must count once, got seen=%d", snap.SeenBatches)
	}
}

func TestMonitorFreshnessMonotonic(t *testing.T) {
	base := time.Now()
	current := base
	m := NewMonitor()
	m.now = func() time.Time { return current }

	observeBatch(m, "b1", 1, base.Add(-time.Hour), Record{"id": 1, "amount": 1.0})
	lag1 := m.Snapshot().FreshnessLag
	// 乱序到达一个更旧的事件时间：新鲜度不得倒退
	observeBatch(m, "b2", 2, base.Add(-2*time.Hour), Record{"id": 2, "amount": 1.0})
	lag2 := m.Snapshot().FreshnessLag
	t.Logf("lag1=%s lag2=%s", lag1, lag2)
	if lag2 != lag1 {
		t.Fatalf("freshness must not regress on older event time: %s -> %s", lag1, lag2)
	}
	// 更新的事件时间应推进新鲜度
	observeBatch(m, "b3", 3, base.Add(-time.Minute), Record{"id": 3, "amount": 1.0})
	lag3 := m.Snapshot().FreshnessLag
	t.Logf("lag3=%s", lag3)
	if lag3 >= lag1 {
		t.Fatalf("fresher event time must reduce lag: %s -> %s", lag1, lag3)
	}
}

func TestMonitorCompleteness(t *testing.T) {
	m := NewMonitor()
	now := time.Now()
	// 4 条记录，amount 缺失 1 条
	observeBatch(m, "b1", 1, now,
		Record{"id": 1, "amount": 1.0},
		Record{"id": 2, "amount": 2.0},
		Record{"id": 3, "amount": 3.0},
		Record{"id": 4}, // amount 缺失
	)
	snap := m.Snapshot()
	t.Logf("completeness=%v", snap.Completeness)
	if got := snap.Completeness["id"]; got != 1.0 {
		t.Fatalf("id completeness want 1.0, got %v", got)
	}
	if got := snap.Completeness["amount"]; got != 0.75 {
		t.Fatalf("amount completeness want 0.75, got %v", got)
	}
}

func TestMonitorDriftAlert(t *testing.T) {
	m := NewMonitor()
	now := time.Now()
	// 建立基线：12 批均值约 10
	for i := 1; i <= 12; i++ {
		observeBatch(m, fmt.Sprintf("base-%d", i), uint64(i), now,
			Record{"id": i, "amount": 10.0})
	}
	snap := m.Snapshot()
	t.Logf("baseline=%v alerts=%v", snap.DriftBaseline, snap.DriftAlerts)
	if len(snap.DriftAlerts) != 0 {
		t.Fatalf("stable input must not alert, got %v", snap.DriftAlerts)
	}
	// 漂移批次：均值 100
	observeBatch(m, "drift-1", 13, now, Record{"id": 100, "amount": 100.0})
	snap = m.Snapshot()
	t.Logf("baseline=%v alerts=%v", snap.DriftBaseline, snap.DriftAlerts)
	if len(snap.DriftAlerts) != 1 {
		t.Fatalf("drift must alert once, got %v", snap.DriftAlerts)
	}
	// 重复提交漂移批次不得重复告警
	observeBatch(m, "drift-1", 13, now, Record{"id": 100, "amount": 100.0})
	if got := len(m.Snapshot().DriftAlerts); got != 1 {
		t.Fatalf("duplicate drift batch must not re-alert, got %d alerts", got)
	}
}
