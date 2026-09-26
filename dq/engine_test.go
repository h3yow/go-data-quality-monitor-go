package dq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func engineContract(version int) Contract {
	return Contract{
		Version: version,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "amount", Type: TypeFloat, Required: true},
		},
		KeyGroup: []string{"id"},
	}
}

// TestEngineConcurrentSubmit 并发提交不同批次：结果、指标与基线需一致。
func TestEngineConcurrentSubmit(t *testing.T) {
	e := NewEngine(engineContract(1))
	const workers = 8
	const batchesPerWorker = 20
	now := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < batchesPerWorker; i++ {
				seq := uint64(w*batchesPerWorker + i + 1)
				b := Batch{
					ID:        fmt.Sprintf("w%d-b%d", w, i),
					Seq:       seq,
					EventTime: now,
					Records: []Record{
						{"id": int(seq), "amount": 10.0},
						{"id": int(seq) + 100000}, // amount 缺失 -> violation
					},
				}
				res, err := e.Submit(context.Background(), b)
				if err != nil {
					t.Errorf("submit %s: %v", b.ID, err)
					return
				}
				if len(res.Accepted) != 1 || len(res.Violations) != 1 {
					t.Errorf("batch %s: want 1 accepted 1 violation, got %d/%d",
						b.ID, len(res.Accepted), len(res.Violations))
				}
			}
		}(w)
	}
	wg.Wait()

	m := e.Metrics()
	t.Logf("watermark=%d seen=%d accepted=%d violations=%d completeness=%v",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations, m.Completeness)
	total := workers * batchesPerWorker
	if m.SeenBatches != total {
		t.Fatalf("want %d seen batches, got %d", total, m.SeenBatches)
	}
	if m.TotalAccepted != total || m.TotalViolations != total {
		t.Fatalf("want %d/%d accepted/violations, got %d/%d",
			total, total, m.TotalAccepted, m.TotalViolations)
	}
	if m.Watermark != uint64(total) {
		t.Fatalf("watermark want %d, got %d", total, m.Watermark)
	}
	if got := m.Completeness["amount"]; got != 0.5 {
		t.Fatalf("amount completeness want 0.5, got %v", got)
	}
}

// TestEngineConcurrentDuplicateSubmit 并发重复提交相同批次：指标不得重复计数。
func TestEngineConcurrentDuplicateSubmit(t *testing.T) {
	e := NewEngine(engineContract(1))
	now := time.Now()
	b := Batch{ID: "dup-batch", Seq: 1, EventTime: now,
		Records: []Record{{"id": 1, "amount": 1.0}}}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.Submit(context.Background(), b)
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			if len(res.Accepted) != 1 {
				t.Errorf("duplicate submit must reproduce same result, got %d accepted", len(res.Accepted))
			}
		}()
	}
	wg.Wait()

	m := e.Metrics()
	t.Logf("seen=%d accepted=%d watermark=%d", m.SeenBatches, m.TotalAccepted, m.Watermark)
	if m.SeenBatches != 1 || m.TotalAccepted != 1 {
		t.Fatalf("concurrent duplicates must count once, got seen=%d accepted=%d",
			m.SeenBatches, m.TotalAccepted)
	}
}

// TestEngineContractEvolution 覆盖三种兼容策略。
func TestEngineContractEvolution(t *testing.T) {
	incompatible := engineContract(2)
	incompatible.Fields = incompatible.Fields[:1] // 删除 amount 字段 -> 不兼容

	// Reject
	e := NewEngine(engineContract(1), WithCompatMode(CompatReject))
	if err := e.Evolve(incompatible); err == nil {
		t.Fatal("reject mode must fail on incompatible evolution")
	}
	if got := e.Contract().Version; got != 1 {
		t.Fatalf("rejected evolution must keep contract v1, got v%d", got)
	}
	t.Logf("reject mode: kept v%d, err expected", e.Contract().Version)

	// Bypass
	e = NewEngine(engineContract(1), WithCompatMode(CompatBypass))
	if err := e.Evolve(incompatible); err != nil {
		t.Fatalf("bypass mode must not error, got %v", err)
	}
	if got := e.Contract().Version; got != 1 {
		t.Fatalf("bypassed evolution must keep contract v1, got v%d", got)
	}
	t.Logf("bypass mode: kept v%d, warnings=%v", e.Contract().Version, e.Warnings())
	if len(e.Warnings()) != 1 {
		t.Fatalf("bypass must record a warning, got %v", e.Warnings())
	}

	// Warn
	e = NewEngine(engineContract(1), WithCompatMode(CompatWarn))
	if err := e.Evolve(incompatible); err != nil {
		t.Fatalf("warn mode must not error, got %v", err)
	}
	if got := e.Contract().Version; got != 2 {
		t.Fatalf("warn mode must apply v2, got v%d", got)
	}
	t.Logf("warn mode: applied v%d, warnings=%v", e.Contract().Version, e.Warnings())
	if len(e.Warnings()) != 1 {
		t.Fatalf("warn must record a warning, got %v", e.Warnings())
	}

	// 兼容演进：新增可选字段，任何策略都应接受
	e = NewEngine(engineContract(1), WithCompatMode(CompatReject))
	compatible := engineContract(2)
	compatible.Fields = append(compatible.Fields, FieldSpec{Name: "note", Type: TypeString})
	if err := e.Evolve(compatible); err != nil {
		t.Fatalf("compatible evolution must succeed, got %v", err)
	}
	t.Logf("compatible evolution to v%d accepted", e.Contract().Version)

	// 版本倒退必须拒绝
	if err := e.Evolve(engineContract(2)); err == nil {
		t.Fatal("non-increasing version must be rejected")
	}
}

// TestEngineContractEvolutionAffectsValidation 演进后新批次按新契约校验。
func TestEngineContractEvolutionAffectsValidation(t *testing.T) {
	e := NewEngine(engineContract(1))
	v2 := engineContract(2)
	maxLen := 3
	v2.Fields = append(v2.Fields, FieldSpec{
		Name: "tag", Type: TypeString, Required: true, Constraint: &Constraint{MaxLen: &maxLen},
	})
	// 新增必需字段不兼容，使用 Warn 策略强制应用
	e2 := NewEngine(engineContract(1), WithCompatMode(CompatWarn))
	if err := e2.Evolve(v2); err != nil {
		t.Fatalf("evolve: %v", err)
	}
	res, err := e2.Submit(context.Background(), Batch{
		ID: "post-evolve", Seq: 1, EventTime: time.Now(),
		Records: []Record{{"id": 1, "amount": 1.0}}, // 缺新必需字段 tag
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("batch=%s contract=v%d violations=%+v", res.BatchID, res.ContractVersion, res.Violations)
	if res.ContractVersion != 2 {
		t.Fatalf("post-evolution batch must use v2, got v%d", res.ContractVersion)
	}
	if len(res.Violations) != 1 || res.Violations[0].Reason != ReasonFieldMissing {
		t.Fatalf("want field_missing for tag, got %+v", res.Violations)
	}
	_ = e
}

// TestEngineMalformedInput 畸形输入不得破坏引擎状态。
func TestEngineMalformedInput(t *testing.T) {
	e := NewEngine(engineContract(1),
		WithCustomValidators(func(ctx context.Context, r Record) error {
			panic("always panics")
		}),
		WithValidatorTimeout(50*time.Millisecond),
	)
	// 空批次
	res, err := e.Submit(context.Background(), Batch{ID: "empty", Seq: 1, EventTime: time.Now()})
	if err != nil {
		t.Fatalf("empty batch must not error, got %v", err)
	}
	t.Logf("batch=%s accepted=%d violations=%d", res.BatchID, len(res.Accepted), len(res.Violations))

	// 全部记录触发自定义校验 panic
	res, err = e.Submit(context.Background(), Batch{
		ID: "panic-batch", Seq: 2, EventTime: time.Now(),
		Records: []Record{{"id": 1, "amount": 1.0}, {"id": 2, "amount": 2.0}},
	})
	if err != nil {
		t.Fatalf("panic in custom validator must not leak, got %v", err)
	}
	t.Logf("batch=%s accepted=%d violations=%d first-reason=%s",
		res.BatchID, len(res.Accepted), len(res.Violations), res.Violations[0].Reason)
	if len(res.Accepted) != 0 || len(res.Violations) != 2 {
		t.Fatalf("want 0 accepted 2 violations, got %d/%d", len(res.Accepted), len(res.Violations))
	}

	// 引擎状态仍然可用
	res, err = e.Submit(context.Background(), Batch{
		ID: "after-fault", Seq: 3, EventTime: time.Now(),
		Records: []Record{{"id": 3, "amount": 3.0}},
	})
	if err != nil {
		t.Fatalf("engine must survive faults, got %v", err)
	}
	m := e.Metrics()
	t.Logf("after faults: watermark=%d seen=%d violations=%d",
		m.Watermark, m.SeenBatches, m.TotalViolations)
	if m.Watermark != 3 || m.SeenBatches != 3 {
		t.Fatalf("engine state corrupted: %+v", m)
	}

	// ctx 取消
	if _, err := e.Submit(canceledCtx(), Batch{ID: "cancelled", Seq: 4}); err == nil {
		t.Fatal("cancelled context must return error")
	}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
