package dq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func testContract() Contract {
	min, max := 0.0, 150.0
	maxLen := 8
	return Contract{
		Version: 1,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "name", Type: TypeString, Required: true, Constraint: &Constraint{MaxLen: &maxLen}},
			{Name: "age", Type: TypeInt, Required: false, Constraint: &Constraint{Min: &min, Max: &max}},
			{Name: "score", Type: TypeFloat, Required: false},
		},
		KeyGroup: []string{"id"},
	}
}

func TestValidateFieldMissing(t *testing.T) {
	v := NewValidator(testContract(), nil, 0)
	b := Batch{ID: "b-missing", Seq: 1, Records: []Record{
		{"id": 1}, // 缺 name
	}}
	res := v.Validate(b)
	t.Logf("batch=%s contract=v%d accepted=%d violations=%d",
		res.BatchID, res.ContractVersion, len(res.Accepted), len(res.Violations))
	for _, viol := range res.Violations {
		t.Logf("  row=%d reason=%s field=%s detail=%s", viol.Row, viol.Reason, viol.Field, viol.Detail)
	}
	if len(res.Accepted) != 0 || len(res.Violations) != 1 {
		t.Fatalf("want 0 accepted 1 violation, got %d/%d", len(res.Accepted), len(res.Violations))
	}
	if res.Violations[0].Reason != ReasonFieldMissing || res.Violations[0].Field != "name" {
		t.Fatalf("unexpected violation: %+v", res.Violations[0])
	}
}

func TestValidateTypeConflict(t *testing.T) {
	v := NewValidator(testContract(), nil, 0)
	b := Batch{ID: "b-type", Seq: 1, Records: []Record{
		{"id": "not-an-int", "name": "ok"},
	}}
	res := v.Validate(b)
	t.Logf("batch=%s contract=v%d violations=%v", res.BatchID, res.ContractVersion, res.Violations)
	if len(res.Violations) != 1 || res.Violations[0].Reason != ReasonTypeConflict {
		t.Fatalf("want type_conflict, got %+v", res.Violations)
	}
}

func TestValidateConstraintBreach(t *testing.T) {
	v := NewValidator(testContract(), nil, 0)
	b := Batch{ID: "b-constraint", Seq: 1, Records: []Record{
		{"id": 1, "name": "this-name-is-way-too-long"},   // MaxLen 越界
		{"id": 2, "name": "ok", "age": 200},               // Max 越界
		{"id": 3, "name": "ok", "age": -1},                // Min 越界
		{"id": 4, "name": "ok", "age": 30, "score": 9.5},  // 合法
	}}
	res := v.Validate(b)
	for _, viol := range res.Violations {
		t.Logf("batch=%s row=%d reason=%s field=%s detail=%s",
			viol.BatchID, viol.Row, viol.Reason, viol.Field, viol.Detail)
	}
	if len(res.Accepted) != 1 {
		t.Fatalf("want 1 accepted, got %d", len(res.Accepted))
	}
	if len(res.Violations) != 3 {
		t.Fatalf("want 3 violations, got %d: %+v", len(res.Violations), res.Violations)
	}
	for _, viol := range res.Violations {
		if viol.Reason != ReasonConstraintBreach {
			t.Fatalf("want constraint_breach, got %s", viol.Reason)
		}
	}
}

func TestValidateDuplicateKey(t *testing.T) {
	v := NewValidator(testContract(), nil, 0)
	b := Batch{ID: "b-dup", Seq: 1, Records: []Record{
		{"id": 1, "name": "a"},
		{"id": 2, "name": "b"},
		{"id": 1, "name": "c"}, // 与第 0 行键重复
	}}
	res := v.Validate(b)
	t.Logf("batch=%s contract=v%d accepted=%d violations=%+v",
		res.BatchID, res.ContractVersion, len(res.Accepted), res.Violations)
	if len(res.Accepted) != 2 || len(res.Violations) != 1 {
		t.Fatalf("want 2 accepted 1 violation, got %d/%d", len(res.Accepted), len(res.Violations))
	}
	if res.Violations[0].Reason != ReasonDuplicateKey || res.Violations[0].Row != 2 {
		t.Fatalf("unexpected violation: %+v", res.Violations[0])
	}
}

func TestValidateDeterministic(t *testing.T) {
	v := NewValidator(testContract(), nil, 0)
	b := Batch{ID: "b-det", Seq: 1, Records: []Record{
		{"id": 1, "name": "a", "age": 200},
		{"name": "no-id"},
		{"id": 1, "name": "dup"},
		{"id": "bad", "name": "x"},
	}}
	r1 := v.Validate(b)
	r2 := v.Validate(b)
	if fmt.Sprintf("%+v", r1) != fmt.Sprintf("%+v", r2) {
		t.Fatalf("same contract + same input must reproduce same result:\n%+v\n%+v", r1, r2)
	}
	t.Logf("batch=%s contract=v%d deterministic result: accepted=%d violations=%d",
		r1.BatchID, r1.ContractVersion, len(r1.Accepted), len(r1.Violations))
}

func TestCustomValidatorFaultIsolation(t *testing.T) {
	panicker := func(ctx context.Context, r Record) error {
		panic("boom")
	}
	slower := func(ctx context.Context, r Record) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}
	rejecter := func(ctx context.Context, r Record) error {
		return errors.New("business rule failed")
	}

	v := NewValidator(testContract(), []CustomValidator{panicker}, 0)
	res := v.Validate(Batch{ID: "b-panic", Seq: 1, Records: []Record{{"id": 1, "name": "a"}}})
	t.Logf("batch=%s violations=%+v", res.BatchID, res.Violations)
	if len(res.Violations) != 1 || res.Violations[0].Reason != ReasonValidatorFault {
		t.Fatalf("panic must become validator_fault, got %+v", res.Violations)
	}

	v = NewValidator(testContract(), []CustomValidator{slower}, 50*time.Millisecond)
	start := time.Now()
	res = v.Validate(Batch{ID: "b-timeout", Seq: 1, Records: []Record{{"id": 1, "name": "a"}}})
	elapsed := time.Since(start)
	t.Logf("batch=%s elapsed=%s violations=%+v", res.BatchID, elapsed, res.Violations)
	if len(res.Violations) != 1 || res.Violations[0].Reason != ReasonValidatorFault {
		t.Fatalf("timeout must become validator_fault, got %+v", res.Violations)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("timeout isolation too slow: %s", elapsed)
	}

	v = NewValidator(testContract(), []CustomValidator{rejecter}, 0)
	res = v.Validate(Batch{ID: "b-reject", Seq: 1, Records: []Record{{"id": 1, "name": "a"}}})
	t.Logf("batch=%s violations=%+v", res.BatchID, res.Violations)
	if len(res.Violations) != 1 || res.Violations[0].Reason != ReasonCustomCheck {
		t.Fatalf("custom error must become custom_check, got %+v", res.Violations)
	}
}
