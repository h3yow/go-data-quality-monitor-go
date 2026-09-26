package engine_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/h3yow/go-data-quality-monitor-go/internal/contract"
	"github.com/h3yow/go-data-quality-monitor-go/internal/engine"
	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
	"github.com/h3yow/go-data-quality-monitor-go/internal/monitor"
	"github.com/h3yow/go-data-quality-monitor-go/internal/source"
	"github.com/h3yow/go-data-quality-monitor-go/internal/validate"
)

// TestEndToEndSourceToEngine 从内存数据源并发/重复/乱序投喂，
// 校验最终指标与逐批次结论的一致性。
func TestEndToEndSourceToEngine(t *testing.T) {
	var clock int64 = 5_000
	e := engine.New(engine.Options{
		Policy:   contract.DefaultPolicy(),
		Monitor:  monitor.Options{AllowedLatenessMs: 500, FreezeAfter: 50, DriftZ: 2.5, FreshnessAlertMs: 0},
		Validate: validate.Options{RuleTimeout: 50 * time.Millisecond},
		NowFn:    func() int64 { clock += 10; return clock },
	})

	c1 := model.Contract{Name: "default", Version: "1.0.0", Fields: []model.FieldSpec{
		{Name: "id", Type: model.TypeInt, Required: true, Nullable: false},
		{Name: "price", Type: model.TypeFloat, Required: true, Nullable: false, Min: f64(0), Max: f64(1000)},
	}, KeyFields: []string{"id"}}
	c11 := c1
	c11.Version = "1.1.0"
	c11.Fields = append(c11.Fields, model.FieldSpec{Name: "label", Type: model.TypeString, Required: false, Nullable: true})
	for _, c := range []model.Contract{c1, c11} {
		if err := e.RegisterContract(c); err != nil {
			t.Fatal(err)
		}
	}

	src := source.NewMemorySource()
	// 正常批次（事件时间 100..400）。
	for i := int64(1); i <= 4; i++ {
		src.Push(model.Batch{
			BatchID: "ok-" + itoa(i), ContractV: "1.0.0", EventTimeMillis: i * 100,
			Records: []model.Record{
				{"id": i*10 + 1, "price": float64(100 + i)},
				{"id": i*10 + 2, "price": float64(102 + i)},
			},
		})
	}
	// 畸形批次（越界 + 缺失 + 类型冲突）。
	src.Push(model.Batch{BatchID: "bad-1", ContractV: "1.0.0", EventTimeMillis: 500, Records: []model.Record{
		{"id": int64(9001), "price": 5000.0},
		{"price": 12.0},
		{"id": "nope", "price": 12.0},
	}})
	// 乱序批次（事件时间远早于水位）。
	src.Push(model.Batch{BatchID: "ooo-1", ContractV: "1.0.0", EventTimeMillis: 50, Records: []model.Record{
		{"id": int64(8001), "price": 9.0},
	}})
	// 契约兼容演进批次。
	src.Push(model.Batch{BatchID: "evo-1", ContractV: "1.1.0", EventTimeMillis: 600, Records: []model.Record{
		{"id": int64(7001), "price": 50.0, "label": "new"},
	}})

	// 乱序重放：逆序提交，并把第一个批次再重复提交一次。
	ch := src.StreamReverse(context.Background())
	var first model.Batch
	n := 0
	for b := range ch {
		if n == 0 {
			first = b
		}
		r := e.Submit(b)
		log.Printf("[IT] batch=%s v=%s outcome=%s valid=%d invalid=%d wm=%d reasons=%v alerts=%d",
			r.BatchID, r.ContractV, r.Outcome, r.ValidCount, r.InvalidCount,
			r.MetricsAfter.WatermarkMs, r.ReasonCounts, len(r.Alerts))
		n++
	}
	dup := e.Submit(first)
	if dup.Outcome != model.OutcomeDuplicate {
		t.Fatalf("resubmitted first batch should be duplicate, got %s", dup.Outcome)
	}
	// 再重复一次，确认幂等。
	dup2 := e.Submit(first)
	if dup2.Outcome != model.OutcomeDuplicate {
		t.Fatalf("second resubmit should be duplicate, got %s", dup2.Outcome)
	}

	snap, ok := e.Snapshot("default")
	if !ok {
		t.Fatal("snapshot missing")
	}
	log.Printf("[IT] SNAPSHOT watermark=%d valid_records=%d invalid_records=%d accepted_batches=%d rejected_batches=%d sideways=%d completeness=%.3f active=%s",
		snap.WatermarkMs, snap.TotalAccepted, snap.TotalRejected, snap.AcceptedBatches,
		snap.RejectedBatches, snap.SidewaysBatches, snap.CompletenessRatio, snap.ActiveVersion)

	if snap.ActiveVersion != "1.1.0" {
		t.Fatalf("active version=%s", snap.ActiveVersion)
	}
	// 水位 = 干净批次的最大事件时间 600（bad-1 500 被拒不推进；ooo-1 50 迟到不推进）。
	if snap.WatermarkMs != 600 {
		t.Fatalf("watermark=%d want 600", snap.WatermarkMs)
	}
	// 合法记录：4 批 *2 + ooo-1 1 + evo-1 1 = 10；非法 3。
	if snap.TotalAccepted != 10 {
		t.Fatalf("valid records=%d want 10", snap.TotalAccepted)
	}
	if snap.TotalRejected != 3 {
		t.Fatalf("invalid records=%d want 3", snap.TotalRejected)
	}
	// 7 个唯一批次：4 正常 + evo-1 + ooo-1(迟到但合法) = 6 accepted，bad-1 = 1 rejected。
	if snap.AcceptedBatches != 6 || snap.RejectedBatches != 1 {
		t.Fatalf("batch counts: accepted=%d rejected=%d", snap.AcceptedBatches, snap.RejectedBatches)
	}
	if snap.CompletenessRatio >= 1.0 {
		t.Fatalf("completeness should reflect invalid records: %.3f", snap.CompletenessRatio)
	}
}

func f64(x float64) *float64 { return &x }

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}
