// Command demo 使用内存数据源演示数据质量校验与增量监控引擎：
// 正常批次、畸形输入、乱序/重复批次、契约演进与并发提交。
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/h3yow/go-data-quality-monitor-go/internal/contract"
	"github.com/h3yow/go-data-quality-monitor-go/internal/engine"
	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
	"github.com/h3yow/go-data-quality-monitor-go/internal/monitor"
	"github.com/h3yow/go-data-quality-monitor-go/internal/source"
)

func f64(x float64) *float64 { return &x }

func main() {
	var clock int64 = 1_700_000_000_000
	e := engine.New(engine.Options{
		Policy: contract.Policy{
			OnBreaking:   contract.ModeSideways,
			OnCompatible: contract.ModeAlert,
			OnDowngrade:  contract.ModeAlert,
		},
		Monitor: monitor.Options{
			AllowedLatenessMs: 30_000,
			FreezeAfter:       200,
			DriftZ:            3.0,
			FreshnessAlertMs:  120_000,
		},
		NowFn: func() int64 { clock += 7; return clock },
	})

	contracts := []model.Contract{
		{Name: "default", Version: "1.0.0", KeyFields: []string{"id"}, Fields: []model.FieldSpec{
			{Name: "id", Type: model.TypeInt, Required: true, Nullable: false},
			{Name: "price", Type: model.TypeFloat, Required: true, Nullable: false, Min: f64(0), Max: f64(1000)},
		}},
		{Name: "default", Version: "1.1.0", KeyFields: []string{"id"}, Fields: []model.FieldSpec{
			{Name: "id", Type: model.TypeInt, Required: true, Nullable: false},
			{Name: "price", Type: model.TypeFloat, Required: true, Nullable: false, Min: f64(0), Max: f64(1000)},
			{Name: "label", Type: model.TypeString, Required: false, Nullable: true},
		}},
		{Name: "default", Version: "2.0.0", KeyFields: []string{"id"}, Fields: []model.FieldSpec{
			{Name: "id", Type: model.TypeString, Required: true, Nullable: false}, // 破坏性：int -> string
			{Name: "price", Type: model.TypeFloat, Required: true, Nullable: false, Min: f64(0), Max: f64(1000)},
		}},
	}
	for _, c := range contracts {
		if err := e.RegisterContract(c); err != nil {
			panic(err)
		}
	}

	src := source.NewMemorySource()
	base := int64(1_700_000_000_000)
	for i := int64(1); i <= 3; i++ {
		src.Push(model.Batch{
			BatchID: fmt.Sprintf("batch-%d", i), ContractV: "1.0.0",
			EventTimeMillis: base + i*10_000,
			Records: []model.Record{
				{"id": i * 10, "price": float64(100 + i)},
				{"id": i*10 + 1, "price": float64(200 + i)},
			},
		})
	}
	// 畸形批次：越界 / 缺失 / 类型冲突。
	src.Push(model.Batch{BatchID: "batch-bad", ContractV: "1.0.0", EventTimeMillis: base + 40_000, Records: []model.Record{
		{"id": int64(9001), "price": 5000.0},
		{"price": 12.0},
		{"id": "bad-id", "price": 12.0},
	}})
	// 乱序迟到批次。
	src.Push(model.Batch{BatchID: "batch-late", ContractV: "1.0.0", EventTimeMillis: base + 1_000, Records: []model.Record{
		{"id": int64(8001), "price": 9.0},
	}})
	// 兼容演进批次。
	src.Push(model.Batch{BatchID: "batch-v11", ContractV: "1.1.0", EventTimeMillis: base + 50_000, Records: []model.Record{
		{"id": int64(7001), "price": 50.0, "label": "shiny"},
	}})
	// 破坏性演进批次（按策略旁路）。
	src.Push(model.Batch{BatchID: "batch-v2", ContractV: "2.0.0", EventTimeMillis: base + 60_000, Records: []model.Record{
		{"id": "x1", "price": 60.0},
	}})

	submit := func(tag string, b model.Batch) {
		r := e.Submit(b)
		fmt.Printf("[%s] batch=%-10s v=%s outcome=%-9s valid=%d invalid=%d watermark=%d",
			tag, r.BatchID, r.ContractV, r.Outcome, r.ValidCount, r.InvalidCount, r.MetricsAfter.WatermarkMs)
		if len(r.ReasonCounts) > 0 {
			fmt.Printf(" reasons=%v", r.ReasonCounts)
		}
		if len(r.Alerts) > 0 {
			kinds := make([]string, 0, len(r.Alerts))
			for _, a := range r.Alerts {
				kinds = append(kinds, a.Kind)
			}
			fmt.Printf(" alerts=%v", kinds)
		}
		fmt.Println()
	}

	// 1) 顺序提交（含重复提交 batch-1）。
	ctx := context.Background()
	var first model.Batch
	i := 0
	for b := range src.Stream(ctx) {
		if i == 0 {
			first = b
		}
		submit("seq", b)
		i++
	}
	submit("dup", first)
	submit("dup", first)

	// 2) 并发提交全新批次。
	var wg sync.WaitGroup
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			b := model.Batch{
				BatchID: fmt.Sprintf("conc-%d", g), ContractV: "1.1.0",
				EventTimeMillis: base + 70_000 + int64(g)*1000,
				Records: []model.Record{
					{"id": int64(6000 + g), "price": float64(300 + g)},
				},
			}
			submit("par", b)
		}(g)
	}
	wg.Wait()

	snap, _ := e.Snapshot("default")
	fmt.Println("----- FINAL SNAPSHOT -----")
	fmt.Printf("active_version=%s watermark=%d\n", snap.ActiveVersion, snap.WatermarkMs)
	fmt.Printf("accepted_records=%d rejected_records=%d completeness=%.3f\n",
		snap.TotalAccepted, snap.TotalRejected, snap.CompletenessRatio)
	fmt.Printf("accepted_batches=%d rejected_batches=%d sideways_batches=%d duplicate_runs=2(returned, not counted)\n",
		snap.AcceptedBatches, snap.RejectedBatches, snap.SidewaysBatches)
	fmt.Printf("freshness_lag_ms=%d distributions=%d\n", snap.FreshnessLagMs, len(snap.Distributions))
}
