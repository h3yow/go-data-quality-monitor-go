package dq

import (
	"context"
	"testing"
	"time"
)

func ptrf(v float64) *float64 { return &v }
func ptri(v int) *int         { return &v }

// field 从变更列表中查找指定字段+类别的变更，找不到返回 nil。
func findChange(cs []Change, field string, kind ChangeKind) *Change {
	for i := range cs {
		if cs[i].Field == field && cs[i].Kind == kind {
			return &cs[i]
		}
	}
	return nil
}

func baseEvoContract() Contract {
	return Contract{
		Version: 1,
		Fields: []FieldSpec{
			{Name: "id", Type: TypeInt, Required: true},
			{Name: "age", Type: TypeInt, Required: false, Constraint: &Constraint{Min: ptrf(0), Max: ptrf(100)}},
			{Name: "name", Type: TypeString, Required: true, Constraint: &Constraint{MaxLen: ptri(10)}},
		},
		KeyGroup: []string{"id"},
	}
}

// TestEvolveRequiredTightening 可选 -> 必填必须判为不兼容；必填 -> 可选兼容。
func TestEvolveRequiredTightening(t *testing.T) {
	old := baseEvoContract()

	nextTight := baseEvoContract()
	nextTight.Version = 2
	nextTight.Fields[1].Required = true // age 可选 -> 必填

	changes := AnalyzeEvolution(old, nextTight)
	c := findChange(changes, "age", ChangeRequiredTightened)
	if c == nil || c.Compatible {
		t.Fatalf("optional -> required must be incompatible tightening, got %+v", changes)
	}
	t.Logf("收紧: %s", c)

	// 三种策略：reject / bypass / warn
	e := NewEngine(old, WithCompatMode(CompatReject))
	if rep := e.EvolveDetailed(nextTight); rep.Action != EvolutionRejected || rep.Err == nil {
		t.Fatalf("reject: want rejected+err, got action=%s err=%v", rep.Action, rep.Err)
	} else {
		t.Logf("reject action=%s reason=%v", rep.Action, rep.Err)
	}
	if v := e.Contract().Version; v != 1 {
		t.Fatalf("rejected evolution keeps v1, got v%d", v)
	}

	e = NewEngine(old, WithCompatMode(CompatBypass))
	rep := e.EvolveDetailed(nextTight)
	if rep.Action != EvolutionBypassed || e.Contract().Version != 1 || len(rep.Warnings) != 1 {
		t.Fatalf("bypass: want bypassed+v1+warning, got action=%s v%d warn=%v",
			rep.Action, e.Contract().Version, rep.Warnings)
	}
	t.Logf("bypass action=%s warning=%v", rep.Action, rep.Warnings)

	e = NewEngine(old, WithCompatMode(CompatWarn))
	rep = e.EvolveDetailed(nextTight)
	if rep.Action != EvolutionApplied || e.Contract().Version != 2 || len(rep.Warnings) != 1 {
		t.Fatalf("warn: want applied+v2+warning, got action=%s v%d", rep.Action, e.Contract().Version)
	}
	t.Logf("warn action=%s warning=%v", rep.Action, rep.Warnings)

	// 必填 -> 可选：兼容，Reject 策略下也直接生效
	nextRelax := baseEvoContract()
	nextRelax.Version = 2
	nextRelax.Fields[2].Required = false // name 必填 -> 可选
	changes = AnalyzeEvolution(old, nextRelax)
	c = findChange(changes, "name", ChangeRequiredRelaxed)
	if c == nil || !c.Compatible {
		t.Fatalf("required -> optional must be compatible, got %+v", changes)
	}
	t.Logf("放宽: %s", c)
	e = NewEngine(old, WithCompatMode(CompatReject))
	if rep := e.EvolveDetailed(nextRelax); rep.Action != EvolutionApplied || rep.Err != nil {
		t.Fatalf("relaxation must apply under reject, got action=%s err=%v", rep.Action, rep.Err)
	}
}

// TestEvolveRangeTightening 数值上下界收窄=不兼容，放宽=兼容。
func TestEvolveRangeTightening(t *testing.T) {
	old := baseEvoContract()

	// min 0 -> 10：收窄(0~10 之间原本合法的值被拒)
	tightMin := baseEvoContract()
	tightMin.Version = 2
	tightMin.Fields[1].Constraint.Min = ptrf(10)
	if c := findChange(AnalyzeEvolution(old, tightMin), "age", ChangeConstraintTightened); c == nil || c.Compatible {
		t.Fatalf("min raised must be tightening, got %+v", AnalyzeEvolution(old, tightMin))
	} else {
		t.Logf("收窄: %s", c)
	}

	// min 0 -> -10：放宽
	relaxMin := baseEvoContract()
	relaxMin.Version = 2
	relaxMin.Fields[1].Constraint.Min = ptrf(-10)
	if c := findChange(AnalyzeEvolution(old, relaxMin), "age", ChangeConstraintRelaxed); c == nil || !c.Compatible {
		t.Fatalf("min lowered must be relaxation, got %+v", AnalyzeEvolution(old, relaxMin))
	} else {
		t.Logf("放宽: %s", c)
	}

	// max 100 -> 90：收窄
	tightMax := baseEvoContract()
	tightMax.Version = 2
	tightMax.Fields[1].Constraint.Max = ptrf(90)
	if c := findChange(AnalyzeEvolution(old, tightMax), "age", ChangeConstraintTightened); c == nil || c.Compatible {
		t.Fatalf("max lowered must be tightening, got %+v", AnalyzeEvolution(old, tightMax))
	} else {
		t.Logf("收窄: %s", c)
	}

	// max 100 -> 200：放宽
	relaxMax := baseEvoContract()
	relaxMax.Version = 2
	relaxMax.Fields[1].Constraint.Max = ptrf(200)
	if c := findChange(AnalyzeEvolution(old, relaxMax), "age", ChangeConstraintRelaxed); c == nil || !c.Compatible {
		t.Fatalf("max raised must be relaxation, got %+v", AnalyzeEvolution(old, relaxMax))
	} else {
		t.Logf("放宽: %s", c)
	}

	// 引入原本不存在的 max：收窄；解除 max：放宽
	introMax := baseEvoContract()
	introMax.Version = 2
	introMax.Fields[1].Constraint = &Constraint{Min: ptrf(0), Max: ptrf(50)}
	if c := findChange(AnalyzeEvolution(old, introMax), "age", ChangeConstraintTightened); c == nil || c.Compatible {
		t.Fatalf("introducing max must be tightening, got %+v", AnalyzeEvolution(old, introMax))
	} else {
		t.Logf("收窄: %s", c)
	}
	dropMax := baseEvoContract()
	dropMax.Version = 2
	dropMax.Fields[1].Constraint = &Constraint{Min: ptrf(0)}
	if c := findChange(AnalyzeEvolution(old, dropMax), "age", ChangeConstraintRelaxed); c == nil || !c.Compatible {
		t.Fatalf("dropping max must be relaxation, got %+v", AnalyzeEvolution(old, dropMax))
	} else {
		t.Logf("放宽: %s", c)
	}

	// 下界放宽 + 上界收窄同时发生：含收紧项，整体不兼容
	mixed := baseEvoContract()
	mixed.Version = 2
	mixed.Fields[1].Constraint = &Constraint{Min: ptrf(-10), Max: ptrf(90)}
	changes := AnalyzeEvolution(old, mixed)
	hasTight, hasRelax := false, false
	for _, c := range changes {
		if c.Field == "age" && c.Kind == ChangeConstraintTightened {
			hasTight = true
		}
		if c.Field == "age" && c.Kind == ChangeConstraintRelaxed {
			hasRelax = true
		}
	}
	if !hasTight || !hasRelax {
		t.Fatalf("mixed change must report both tightened and relaxed items, got %+v", changes)
	}
	rep := NewEngine(old, WithCompatMode(CompatReject)).EvolveDetailed(mixed)
	if rep.Compatible() || rep.Action != EvolutionRejected {
		t.Fatalf("mixed change containing tightening must be rejected, got %s", rep)
	}
	t.Logf("混合变化逐项可解释:\n%s", rep)
}

// TestEvolveMaxLenTightening 长度收窄=不兼容，放宽=兼容。
func TestEvolveMaxLenTightening(t *testing.T) {
	old := baseEvoContract()

	tight := baseEvoContract()
	tight.Version = 2
	tight.Fields[2].Constraint.MaxLen = ptri(5)
	if c := findChange(AnalyzeEvolution(old, tight), "name", ChangeConstraintTightened); c == nil || c.Compatible {
		t.Fatalf("maxlen narrowed must be tightening, got %+v", AnalyzeEvolution(old, tight))
	} else {
		t.Logf("收窄: %s", c)
	}

	relax := baseEvoContract()
	relax.Version = 2
	relax.Fields[2].Constraint.MaxLen = ptri(20)
	if c := findChange(AnalyzeEvolution(old, relax), "name", ChangeConstraintRelaxed); c == nil || !c.Compatible {
		t.Fatalf("maxlen widened must be relaxation, got %+v", AnalyzeEvolution(old, relax))
	} else {
		t.Logf("放宽: %s", c)
	}

	intro := baseEvoContract()
	intro.Version = 2
	intro.Fields[2].Constraint = nil
	// 先构造"无约束"旧版，再演进为带 MaxLen 的新版 -> 收窄
	noLen := intro
	intro2 := baseEvoContract()
	intro2.Version = 2
	if c := findChange(AnalyzeEvolution(noLen, intro2), "name", ChangeConstraintTightened); c == nil || c.Compatible {
		t.Fatalf("introducing maxlen must be tightening, got %+v", AnalyzeEvolution(noLen, intro2))
	} else {
		t.Logf("收窄: %s", c)
	}

	// 数据层面：收窄被拒后，旧版长度 8 的数据仍合法；Warn 强制应用后变为违规
	e := NewEngine(old, WithCompatMode(CompatReject))
	if err := e.Evolve(tight); err == nil {
		t.Fatal("tightened maxlen must be rejected under reject mode")
	}
	res, _ := e.Submit(testCtx(), makeBatch("len-before", 1, Record{"id": 1, "name": "12345678", "age": 30}))
	if len(res.Violations) != 0 {
		t.Fatalf("rejected evolution: length 8 must stay valid, got %+v", res.Violations)
	}
	t.Logf("收紧被拒后 batch=%s contract=v%d accepted=%d", res.BatchID, res.ContractVersion, len(res.Accepted))

	e2 := NewEngine(old, WithCompatMode(CompatWarn))
	if err := e2.Evolve(tight); err != nil {
		t.Fatalf("warn evolve: %v", err)
	}
	res, _ = e2.Submit(testCtx(), makeBatch("len-after", 1, Record{"id": 1, "name": "12345678", "age": 30}))
	if res.ContractVersion != 2 || len(res.Violations) != 1 || res.Violations[0].Reason != ReasonConstraintBreach {
		t.Fatalf("after forced tightening, length 8 must breach constraint, got %+v", res.Violations)
	}
	t.Logf("强制收紧后 batch=%s contract=v%d violations=%v", res.BatchID, res.ContractVersion, res.Violations)
}

// TestEvolveLegacyBreakingChanges 既有的破坏性变化依然被判为不兼容。
func TestEvolveLegacyBreakingChanges(t *testing.T) {
	old := baseEvoContract()

	removed := baseEvoContract()
	removed.Version = 2
	removed.Fields = removed.Fields[:2] // 删除 name
	if c := findChange(AnalyzeEvolution(old, removed), "name", ChangeFieldRemoved); c == nil || c.Compatible {
		t.Fatalf("field removal must be incompatible, got %+v", AnalyzeEvolution(old, removed))
	}

	typeChange := baseEvoContract()
	typeChange.Version = 2
	typeChange.Fields[1].Type = TypeFloat // age int -> float
	if c := findChange(AnalyzeEvolution(old, typeChange), "age", ChangeTypeChanged); c == nil || c.Compatible {
		t.Fatalf("type change must be incompatible, got %+v", AnalyzeEvolution(old, typeChange))
	}

	newReq := baseEvoContract()
	newReq.Version = 2
	newReq.Fields = append(newReq.Fields, FieldSpec{Name: "ssn", Type: TypeString, Required: true})
	if c := findChange(AnalyzeEvolution(old, newReq), "ssn", ChangeFieldAdded); c == nil || c.Compatible {
		t.Fatalf("new required field must be incompatible, got %+v", AnalyzeEvolution(old, newReq))
	}
	newOpt := baseEvoContract()
	newOpt.Version = 2
	newOpt.Fields = append(newOpt.Fields, FieldSpec{Name: "note", Type: TypeString})
	if c := findChange(AnalyzeEvolution(old, newOpt), "note", ChangeFieldAdded); c == nil || !c.Compatible {
		t.Fatalf("new optional field must be compatible, got %+v", AnalyzeEvolution(old, newOpt))
	}

	keyChange := baseEvoContract()
	keyChange.Version = 2
	keyChange.KeyGroup = []string{"id", "name"}
	var foundKey bool
	for _, c := range AnalyzeEvolution(old, keyChange) {
		if c.Kind == ChangeKeyGroupChanged && !c.Compatible {
			foundKey = true
		}
	}
	if !foundKey {
		t.Fatalf("key group change must be incompatible, got %+v", AnalyzeEvolution(old, keyChange))
	}
	t.Log("删除字段/类型变化/新增必需字段/键组变化均仍判为不兼容")
}

// TestEvolveDeterministic 分析结果顺序与内容确定，无变化契约不产出变更项。
func TestEvolveDeterministic(t *testing.T) {
	old := baseEvoContract()
	same := baseEvoContract()
	same.Version = 2
	if cs := AnalyzeEvolution(old, same); len(cs) != 0 {
		t.Fatalf("identical schema must produce no changes, got %+v", cs)
	}
	next := baseEvoContract()
	next.Version = 2
	next.Fields[1].Required = true
	first := AnalyzeEvolution(old, next)
	second := AnalyzeEvolution(old, next)
	if len(first) != len(second) {
		t.Fatal("analysis must be deterministic")
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("analysis mismatch at %d: %+v vs %+v", i, first[i], second[i])
		}
	}
	// 版本未严格递增：rejected 且不产生 Changes
	e := NewEngine(old)
	rep := e.EvolveDetailed(old)
	if rep.Action != EvolutionRejected || rep.Err == nil || len(rep.Changes) != 0 {
		t.Fatalf("non-increasing version: want rejected without changes, got %+v", rep)
	}
	t.Logf("演进分析确定、可解释; 版本倒退: %v", rep.Err)
}

func testCtx() context.Context { return context.Background() }

func makeBatch(id string, seq uint64, recs ...Record) Batch {
	return Batch{ID: id, Seq: seq, Records: recs, EventTime: time.Now()}
}
