package validate

import (
	"testing"
	"time"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

func ptr(f float64) *float64 { return &f }

func testContract() model.Contract {
	return model.Contract{
		Name:    "orders",
		Version: "1.0.0",
		Fields: []model.FieldSpec{
			{Name: "id", Type: model.TypeInt, Required: true, Nullable: false},
			{Name: "name", Type: model.TypeString, Required: true, Nullable: false},
			{Name: "score", Type: model.TypeFloat, Required: false, Nullable: true, Min: ptr(0), Max: ptr(100)},
		},
		KeyFields: []string{"id"},
	}
}

func reasonSet(rr model.RecordResult) map[model.Reason]bool {
	m := map[model.Reason]bool{}
	for _, v := range rr.Violations {
		m[v.Reason] = true
	}
	return m
}

func TestValidateBuiltinReasons(t *testing.T) {
	c := testContract()
	b := model.Batch{BatchID: "b1", ContractV: "1.0.0", Records: []model.Record{
		{"id": int64(1), "name": "ok", "score": 50.0}, // 0 valid
		{"name": "x", "score": 1.0},                   // 1 missing id
		{"id": int64(2), "name": 123},                 // 2 type conflict
		{"id": int64(3), "name": nil},                 // 3 null not allowed
		{"id": int64(4), "name": "y", "score": 200.0}, // 4 constraint max
		{"id": int64(4), "name": "z", "score": -1.0},  // 5 dup key + constraint min
		nil, // 6 malformed
	}}
	res := NewValidator().Validate(c, b, Options{})
	want := []map[model.Reason]bool{
		{},
		{model.ReasonMissingField: true},
		{model.ReasonTypeConflict: true},
		{model.ReasonNullNotAllowed: true},
		{model.ReasonConstraint: true},
		{model.ReasonKeyDuplicate: true, model.ReasonConstraint: true},
		{model.ReasonMalformed: true},
	}
	for i := range b.Records {
		got := reasonSet(res[i])
		for r := range want[i] {
			if !got[r] {
				t.Errorf("record %d: want reason %s, got %+v", i, r, got)
			}
		}
		if len(got) != len(want[i]) {
			t.Errorf("record %d: unexpected reasons %+v want %+v", i, got, want[i])
		}
	}
	if res[0].Valid != true || res[1].Valid != false {
		t.Errorf("valid flags wrong: %+v", res)
	}
}

func TestValidateReproducible(t *testing.T) {
	c := testContract()
	b := model.Batch{BatchID: "b", Records: []model.Record{
		{"id": int64(9), "name": "a"},
		{"id": int64(9), "name": "b"},
		{"name": "c"},
	}}
	v := NewValidator()
	r1 := v.Validate(c, b, Options{})
	r2 := v.Validate(c, b, Options{})
	for i := range r1 {
		if r1[i].Valid != r2[i].Valid || len(r1[i].Violations) != len(r2[i].Violations) {
			t.Fatalf("non-reproducible result at %d", i)
		}
		for j := range r1[i].Violations {
			if r1[i].Violations[j] != r2[i].Violations[j] {
				t.Fatalf("non-reproducible violation at %d/%d", i, j)
			}
		}
	}
}

func TestCustomRulePanicIsolated(t *testing.T) {
	c := testContract()
	b := model.Batch{Records: []model.Record{
		{"id": int64(1), "name": "ok"},
		{"id": int64(2), "name": "boom"},
		{"id": int64(3), "name": "ok2"},
	}}
	opts := Options{
		RuleTimeout: time.Second,
		CustomRules: []CustomRule{func(rec model.Record) (model.Reason, string) {
			if rec["name"] == "boom" {
				panic("kaboom")
			}
			return model.ReasonNone, ""
		}},
	}
	res := NewValidator().Validate(c, b, opts)
	if !res[0].Valid || res[1].Valid || !res[2].Valid {
		t.Fatalf("panic isolation failed: %+v", res)
	}
	if reasonSet(res[1])[model.ReasonCustomPanic] != true {
		t.Fatalf("want custom_rule_panic: %+v", res[1].Violations)
	}
}

func TestCustomRuleTimeout(t *testing.T) {
	c := testContract()
	b := model.Batch{Records: []model.Record{{"id": int64(1), "name": "slow"}}}
	opts := Options{
		RuleTimeout: 20 * time.Millisecond,
		CustomRules: []CustomRule{func(rec model.Record) (model.Reason, string) {
			time.Sleep(2 * time.Second)
			return model.ReasonNone, ""
		}},
	}
	start := time.Now()
	res := NewValidator().Validate(c, b, opts)
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("validator blocked on timed-out rule")
	}
	if !reasonSet(res[0])[model.ReasonCustomTimeout] {
		t.Fatalf("want timeout reason: %+v", res[0].Violations)
	}
}

func TestCustomRuleReject(t *testing.T) {
	c := testContract()
	b := model.Batch{Records: []model.Record{{"id": int64(1), "name": "bad"}}}
	opts := Options{CustomRules: []CustomRule{func(rec model.Record) (model.Reason, string) {
		return model.ReasonCustomReject, "name blacklisted"
	}}}
	res := NewValidator().Validate(c, b, opts)
	if res[0].Valid || !reasonSet(res[0])[model.ReasonCustomReject] {
		t.Fatalf("want custom reject: %+v", res[0])
	}
}
