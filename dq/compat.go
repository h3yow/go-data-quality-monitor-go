package dq

import (
	"fmt"
	"strings"
)

// ChangeKind 区分契约演进中单项变化的类别。
type ChangeKind string

const (
	ChangeFieldAdded           ChangeKind = "field_added"            // 新增字段
	ChangeFieldRemoved         ChangeKind = "field_removed"          // 删除字段
	ChangeFieldTypeChanged     ChangeKind = "field_type_changed"     // 字段类型变化
	ChangeFieldRequiredTighten ChangeKind = "field_required_tighten" // 可选 -> 必需(收紧)
	ChangeFieldRequiredRelax   ChangeKind = "field_required_relax"   // 必需 -> 可选(放宽)
	ChangeConstraintTightened  ChangeKind = "constraint_tightened"   // 约束收窄
	ChangeConstraintRelaxed    ChangeKind = "constraint_relaxed"     // 约束放宽
	ChangeConstraintAdded      ChangeKind = "constraint_added"       // 新增约束(收紧)
	ChangeConstraintRemoved    ChangeKind = "constraint_removed"     // 移除约束(放宽)
	ChangeKeyGroupChanged      ChangeKind = "key_group_changed"      // 键组变化
)

// Change 是契约演进中的一项可解释变化。
type Change struct {
	Kind       ChangeKind // 变化类别
	Field      string     // 涉及字段(键组变化时为空)
	Compatible bool       // 该变化是否兼容(放宽为兼容，收紧为不兼容)
	Detail     string     // 人类可读的原因说明
}

// CompatibilityReport 是一次契约演进判定的完整可解释结果。
type CompatibilityReport struct {
	FromVersion int      // 演进前版本
	ToVersion   int      // 演进后版本
	Changes     []Change // 逐项变化及兼容性判定
}

// Compatible 报告整体是否兼容(所有变化均兼容)。
func (r CompatibilityReport) Compatible() bool {
	for _, c := range r.Changes {
		if !c.Compatible {
			return false
		}
	}
	return true
}

// Incompatible 返回所有不兼容变化。
func (r CompatibilityReport) Incompatible() []Change {
	var out []Change
	for _, c := range r.Changes {
		if !c.Compatible {
			out = append(out, c)
		}
	}
	return out
}

// Summary 返回每项变化一行的人类可读说明，便于日志与测试输出。
func (r CompatibilityReport) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "contract v%d -> v%d: %d change(s), compatible=%v",
		r.FromVersion, r.ToVersion, len(r.Changes), r.Compatible())
	for _, c := range r.Changes {
		verdict := "compatible"
		if !c.Compatible {
			verdict = "INCOMPATIBLE"
		}
		fmt.Fprintf(&b, "\n  - [%s] %s", verdict, c.Detail)
	}
	return b.String()
}

// AnalyzeCompatibility 对 old -> next 的契约演进做逐项判定。
//
// 收紧类变化(会让原本合法的数据突然不合法)判为不兼容：
//   - 删除字段、字段类型变化、键组变化；
//   - 新增必需字段、字段由可选变为必需；
//   - 新增约束、约束收窄(下界抬高/上界压低/最大长度缩短)。
//
// 放宽类变化判为兼容：
//   - 新增可选字段、字段由必需变为可选；
//   - 移除约束、约束放宽(下界降低/上界抬高/最大长度增加)。
func AnalyzeCompatibility(old, next Contract) CompatibilityReport {
	rep := CompatibilityReport{FromVersion: old.Version, ToVersion: next.Version}
	add := func(kind ChangeKind, field string, compatible bool, format string, args ...any) {
		rep.Changes = append(rep.Changes, Change{
			Kind: kind, Field: field, Compatible: compatible,
			Detail: fmt.Sprintf(format, args...),
		})
	}

	oldIdx := old.fieldIndex()
	nextIdx := next.fieldIndex()

	// 按 next 字段声明顺序检查新增与字段级变化，保证输出确定。
	for _, nf := range next.Fields {
		of, ok := oldIdx[nf.Name]
		if !ok {
			if nf.Required {
				add(ChangeFieldAdded, nf.Name, false,
					"field %q added as required: previously valid records lacking it would now be rejected", nf.Name)
			} else {
				add(ChangeFieldAdded, nf.Name, true,
					"field %q added as optional", nf.Name)
			}
			continue
		}
		if of.Type != nf.Type {
			add(ChangeFieldTypeChanged, nf.Name, false,
				"field %q type changed %s -> %s", nf.Name, of.Type, nf.Type)
		}
		switch {
		case !of.Required && nf.Required:
			add(ChangeFieldRequiredTighten, nf.Name, false,
				"field %q changed optional -> required: records missing it would now be rejected", nf.Name)
		case of.Required && !nf.Required:
			add(ChangeFieldRequiredRelax, nf.Name, true,
				"field %q changed required -> optional", nf.Name)
		}
		analyzeConstraint(&rep, nf.Name, of.Constraint, nf.Constraint)
	}

	// 按 old 字段声明顺序检查删除。
	for _, of := range old.Fields {
		if _, ok := nextIdx[of.Name]; !ok {
			add(ChangeFieldRemoved, of.Name, false,
				"field %q removed", of.Name)
		}
	}

	if !equalStrings(old.KeyGroup, next.KeyGroup) {
		add(ChangeKeyGroupChanged, "", false,
			"key group changed %v -> %v", old.KeyGroup, next.KeyGroup)
	}
	return rep
}

// analyzeConstraint 比较同一字段的约束变化并追加判定项。
func analyzeConstraint(rep *CompatibilityReport, field string, old, next *Constraint) {
	add := func(kind ChangeKind, field string, compatible bool, format string, args ...any) {
		rep.Changes = append(rep.Changes, Change{
			Kind: kind, Field: field, Compatible: compatible,
			Detail: fmt.Sprintf(format, args...),
		})
	}
	switch {
	case old == nil && next == nil:
		return
	case old == nil:
		add(ChangeConstraintAdded, field, false,
			"field %q gained constraint %s: previously valid values may now be rejected", field, describeConstraint(next))
		return
	case next == nil:
		add(ChangeConstraintRemoved, field, true,
			"field %q constraint removed", field)
		return
	}
	// 逐项边界比较：下界抬高/上界压低/长度缩短为收紧，反之为放宽。
	if !eqFloatPtr(old.Min, next.Min) {
		switch {
		case old.Min == nil:
			add(ChangeConstraintTightened, field, false,
				"field %q gained min %v", field, *next.Min)
		case next.Min == nil:
			add(ChangeConstraintRelaxed, field, true,
				"field %q min %v removed", field, *old.Min)
		case *next.Min > *old.Min:
			add(ChangeConstraintTightened, field, false,
				"field %q min raised %v -> %v: values in between now rejected", field, *old.Min, *next.Min)
		default:
			add(ChangeConstraintRelaxed, field, true,
				"field %q min lowered %v -> %v", field, *old.Min, *next.Min)
		}
	}
	if !eqFloatPtr(old.Max, next.Max) {
		switch {
		case old.Max == nil:
			add(ChangeConstraintTightened, field, false,
				"field %q gained max %v", field, *next.Max)
		case next.Max == nil:
			add(ChangeConstraintRelaxed, field, true,
				"field %q max %v removed", field, *old.Max)
		case *next.Max < *old.Max:
			add(ChangeConstraintTightened, field, false,
				"field %q max lowered %v -> %v: values in between now rejected", field, *old.Max, *next.Max)
		default:
			add(ChangeConstraintRelaxed, field, true,
				"field %q max raised %v -> %v", field, *old.Max, *next.Max)
		}
	}
	if !eqIntPtr(old.MaxLen, next.MaxLen) {
		switch {
		case old.MaxLen == nil:
			add(ChangeConstraintTightened, field, false,
				"field %q gained max length %d", field, *next.MaxLen)
		case next.MaxLen == nil:
			add(ChangeConstraintRelaxed, field, true,
				"field %q max length %d removed", field, *old.MaxLen)
		case *next.MaxLen < *old.MaxLen:
			add(ChangeConstraintTightened, field, false,
				"field %q max length shortened %d -> %d: longer values now rejected", field, *old.MaxLen, *next.MaxLen)
		default:
			add(ChangeConstraintRelaxed, field, true,
				"field %q max length extended %d -> %d", field, *old.MaxLen, *next.MaxLen)
		}
	}
}

// eqFloatPtr 按值比较两个 float64 指针(同为 nil 视为相等)。
func eqFloatPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// eqIntPtr 按值比较两个 int 指针(同为 nil 视为相等)。
func eqIntPtr(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// describeConstraint 返回约束的紧凑描述。
func describeConstraint(c *Constraint) string {
	var parts []string
	if c.Min != nil {
		parts = append(parts, fmt.Sprintf("min=%v", *c.Min))
	}
	if c.Max != nil {
		parts = append(parts, fmt.Sprintf("max=%v", *c.Max))
	}
	if c.MaxLen != nil {
		parts = append(parts, fmt.Sprintf("maxLen=%d", *c.MaxLen))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
