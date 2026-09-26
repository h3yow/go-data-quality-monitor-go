package contract

import (
	"testing"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

func f(name string, t model.FieldType, req bool) model.FieldSpec {
	return model.FieldSpec{Name: name, Type: t, Required: req, Nullable: true}
}

func TestDiffEvolution(t *testing.T) {
	v1 := model.Contract{Name: "p", Version: "1.0.0", Fields: []model.FieldSpec{
		f("id", model.TypeInt, true), f("age", model.TypeInt, false),
	}, KeyFields: []string{"id"}}
	v2 := model.Contract{Name: "p", Version: "2.0.0", Fields: []model.FieldSpec{
		f("id", model.TypeString, true), f("email", model.TypeString, true),
	}, KeyFields: []string{"id"}}

	changes := Diff(v1, v2)
	kinds := map[ChangeKind]bool{}
	breaking := 0
	for _, c := range changes {
		kinds[c.Kind] = true
		if c.Breaking {
			breaking++
		}
	}
	for _, want := range []ChangeKind{ChangeTypeChanged, ChangeAddedRequiredField, ChangeRemovedField} {
		if !kinds[want] {
			t.Fatalf("missing change %s in %+v", want, changes)
		}
	}
	if breaking != 3 {
		t.Fatalf("want 3 breaking changes, got %d (%+v)", breaking, changes)
	}

	// 新增可选字段为兼容变化。
	v3 := model.Contract{Name: "p", Version: "1.1.0", Fields: []model.FieldSpec{
		f("id", model.TypeInt, true), f("age", model.TypeInt, false), f("nick", model.TypeString, false),
	}, KeyFields: []string{"id"}}
	c11 := Diff(v1, v3)
	if len(c11) != 1 || c11[0].Breaking {
		t.Fatalf("added optional field should be compatible: %+v", c11)
	}

	// Diff 必须可复现。
	c11again := Diff(v1, v3)
	if len(c11) != len(c11again) || c11[0].Kind != c11again[0].Kind {
		t.Fatal("Diff not reproducible")
	}
}

func TestRegistryActive(t *testing.T) {
	r := NewRegistry()
	c := func(v string) model.Contract {
		return model.Contract{Name: "p", Version: v, Fields: []model.FieldSpec{f("id", model.TypeInt, true)}}
	}
	if err := r.Register(c("1.10.0")); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(c("1.2.0")); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(c("1.10.0")); err == nil {
		t.Fatal("duplicate register should fail")
	}
	active, ok := r.Active("p")
	if !ok || active.Version != "1.10.0" {
		t.Fatalf("active=%v ok=%v", active.Version, ok)
	}
}
