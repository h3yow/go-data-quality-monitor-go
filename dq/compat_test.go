package dq

import (
	"strings"
	"testing"
)

func fptr(v float64) *float64 { return &v }
func iptr(v int) *int         { return &v }

// baseContract 提供兼容判定测试的基准契约。
func baseContract() Contract {
	return Contract{
		Version: 1,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "name", Type: TypeString, Required: true, Constraint: &Constraint{MaxLen: iptr(16)}},
			{Name: "amount", Type: TypeFloat, Required: false, Constraint: &Constraint{Min: fptr(0), Max: fptr(100)}},
			{Name: "note", Type: TypeString, Required: false},
		},
		KeyGroup: []string{"id"},
	}
}

// mutate 复制基准契约并按 fn 修改，版本号 +1。
func mutate(fn func(*Contract)) Contract {
	c := baseContract()
	c.Version = 2
	fn(&c)
	return c
}

// field 返回契约中指定名字字段的指针。
func field(c *Contract, name string) *FieldSpec {
	for i := range c.Fields {
		if c.Fields[i].Name == name {
			return &c.Fields[i]
		}
	}
	return nil
}

func TestAnalyzeCompatibilityTightening(t *testing.T) {
	cases := []struct {
		name string
		next Contract
		kind ChangeKind
	}{
		{"可选变必需", mutate(func(c *Contract) { field(c, "amount").Required = true }), ChangeFieldRequiredTighten},
		{"下界抬高", mutate(func(c *Contract) { field(c, "amount").Constraint.Min = fptr(10) }), ChangeConstraintTightened},
		{"上界压低", mutate(func(c *Contract) { field(c, "amount").Constraint.Max = fptr(50) }), ChangeConstraintTightened},
		{"长度缩短", mutate(func(c *Contract) { field(c, "name").Constraint.MaxLen = iptr(8) }), ChangeConstraintTightened},
		{"新增约束", mutate(func(c *Contract) { field(c, "note").Constraint = &Constraint{MaxLen: iptr(4)} }), ChangeConstraintAdded},
		{"新增下界", mutate(func(c *Contract) { field(c, "name").Constraint.Min = fptr(1) }), ChangeConstraintTightened},
		{"新增必需字段", mutate(func(c *Contract) {
			c.Fields = append(c.Fields, FieldSpec{Name: "extra", Type: TypeString, Required: true})
		}), ChangeFieldAdded},
		{"删除字段", mutate(func(c *Contract) { c.Fields = c.Fields[:3] }), ChangeFieldRemoved},
		{"类型变化", mutate(func(c *Contract) { field(c, "amount").Type = TypeInt }), ChangeFieldTypeChanged},
		{"键组变化", mutate(func(c *Contract) { c.KeyGroup = []string{"id", "name"} }), ChangeKeyGroupChanged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := AnalyzeCompatibility(baseContract(), tc.next)
			t.Logf("\n%s", rep.Summary())
			if rep.Compatible() {
				t.Fatalf("expected incompatible, got compatible: %s", rep.Summary())
			}
			found := false
			for _, ch := range rep.Incompatible() {
				if ch.Kind == tc.kind {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected change kind %s in report: %s", tc.kind, rep.Summary())
			}
		})
	}
}

func TestAnalyzeCompatibilityRelaxing(t *testing.T) {
	cases := []struct {
		name string
		next Contract
	}{
		{"必需变可选", mutate(func(c *Contract) { field(c, "name").Required = false })},
		{"下界降低", mutate(func(c *Contract) { field(c, "amount").Constraint.Min = fptr(-10) })},
		{"上界抬高", mutate(func(c *Contract) { field(c, "amount").Constraint.Max = fptr(200) })},
		{"长度放宽", mutate(func(c *Contract) { field(c, "name").Constraint.MaxLen = iptr(32) })},
		{"移除约束", mutate(func(c *Contract) { field(c, "amount").Constraint = nil })},
		{"移除下界", mutate(func(c *Contract) { field(c, "amount").Constraint.Min = nil })},
		{"新增可选字段", mutate(func(c *Contract) {
			c.Fields = append(c.Fields, FieldSpec{Name: "extra", Type: TypeString, Required: false})
		})},
		{"完全不变", mutate(func(c *Contract) {})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rep := AnalyzeCompatibility(baseContract(), tc.next)
			t.Logf("\n%s", rep.Summary())
			if !rep.Compatible() {
				t.Fatalf("expected compatible, got incompatible changes: %s", rep.Summary())
			}
		})
	}
}

func TestCompatibilityReportExplainable(t *testing.T) {
	next := mutate(func(c *Contract) {
		field(c, "amount").Required = true          // 收紧
		field(c, "name").Constraint.MaxLen = iptr(32) // 放宽
	})
	rep := AnalyzeCompatibility(baseContract(), next)
	if rep.FromVersion != 1 || rep.ToVersion != 2 {
		t.Fatalf("unexpected versions: %d -> %d", rep.FromVersion, rep.ToVersion)
	}
	if len(rep.Changes) != 2 {
		t.Fatalf("expected 2 changes, got %d: %s", len(rep.Changes), rep.Summary())
	}
	if len(rep.Incompatible()) != 1 {
		t.Fatalf("expected 1 incompatible change, got %d", len(rep.Incompatible()))
	}
	sum := rep.Summary()
	for _, want := range []string{"v1 -> v2", "optional -> required", "max length extended"} {
		if !strings.Contains(sum, want) {
			t.Fatalf("summary missing %q:\n%s", want, sum)
		}
	}
	t.Logf("\n%s", sum)
}

func TestCheckCompatibilityWrapper(t *testing.T) {
	next := mutate(func(c *Contract) { field(c, "amount").Constraint.Max = fptr(50) })
	items := CheckCompatibility(baseContract(), next)
	if len(items) != 1 || !strings.Contains(items[0], "max lowered") {
		t.Fatalf("unexpected items: %v", items)
	}
	if got := CheckCompatibility(baseContract(), mutate(func(c *Contract) {})); len(got) != 0 {
		t.Fatalf("expected no incompatible items, got %v", got)
	}
}

func TestEvolveWithReportModes(t *testing.T) {
	tightening := mutate(func(c *Contract) { field(c, "name").Constraint.MaxLen = iptr(8) })

	t.Run("拒绝策略", func(t *testing.T) {
		e := NewEngine(baseContract(), WithCompatMode(CompatReject))
		rep, err := e.EvolveWithReport(tightening)
		if err == nil {
			t.Fatal("expected rejection error")
		}
		if rep.Compatible() || len(rep.Incompatible()) != 1 {
			t.Fatalf("report should explain the rejection: %s", rep.Summary())
		}
		if e.Contract().Version != 1 {
			t.Fatalf("contract must stay at v1, got v%d", e.Contract().Version)
		}
		t.Logf("rejected as expected:\n%s", rep.Summary())
	})

	t.Run("旁路策略", func(t *testing.T) {
		e := NewEngine(baseContract(), WithCompatMode(CompatBypass))
		rep, err := e.EvolveWithReport(tightening)
		if err != nil {
			t.Fatalf("bypass must not error: %v", err)
		}
		if rep.Compatible() {
			t.Fatal("report must still flag incompatibility")
		}
		if e.Contract().Version != 1 {
			t.Fatalf("bypassed evolution must not apply, got v%d", e.Contract().Version)
		}
		if len(e.Warnings()) != 1 {
			t.Fatalf("expected 1 bypass warning, got %v", e.Warnings())
		}
		t.Logf("bypassed, warning: %s", e.Warnings()[0])
	})

	t.Run("告警策略", func(t *testing.T) {
		e := NewEngine(baseContract(), WithCompatMode(CompatWarn))
		rep, err := e.EvolveWithReport(tightening)
		if err != nil {
			t.Fatalf("warn must not error: %v", err)
		}
		if e.Contract().Version != 2 {
			t.Fatalf("warn mode must apply v2, got v%d", e.Contract().Version)
		}
		if len(e.Warnings()) != 1 {
			t.Fatalf("expected 1 warn warning, got %v", e.Warnings())
		}
		t.Logf("applied despite incompatibility:\n%s", rep.Summary())
	})
}
