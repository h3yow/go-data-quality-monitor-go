package contract

import (
	"fmt"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// IsCompatible 报告该变更是否向后兼容（不破坏按旧契约产生的数据）。
func (c Change) IsCompatible() bool { return !c.Breaking }

// DefaultPolicy 返回默认策略：破坏性拒绝、兼容变化告警、旧版本告警。
func DefaultPolicy() Policy {
	return Policy{
		OnBreaking:   ModeReject,
		OnCompatible: ModeAlert,
		OnDowngrade:  ModeAlert,
	}
}

// Diff 比较两个契约版本（从 oldC 到 newC），返回字段级差异。
// 差异按字段名与变更类型的稳定顺序排列，保证结论可复现。
func Diff(oldC, newC model.Contract) []Change {
	var changes []Change
	oldByName := map[string]model.FieldSpec{}
	oldOrder := make([]string, 0, len(oldC.Fields))
	for _, f := range oldC.Fields {
		oldByName[f.Name] = f
		oldOrder = append(oldOrder, f.Name)
	}
	newByName := map[string]model.FieldSpec{}
	newOrder := make([]string, 0, len(newC.Fields))
	for _, f := range newC.Fields {
		newByName[f.Name] = f
		newOrder = append(newOrder, f.Name)
	}

	// 以旧契约字段顺序为基线，保证输出确定。
	for _, name := range oldOrder {
		of := oldByName[name]
		nf, ok := newByName[name]
		if !ok {
			changes = append(changes, Change{
				Kind:     ChangeRemovedField,
				Field:    name,
				Detail:   fmt.Sprintf("field %s removed", name),
				Breaking: true,
			})
			continue
		}
		changes = append(changes, diffField(of, nf)...)
	}
	// 新增字段：按新契约中的声明顺序输出。
	for _, name := range newOrder {
		if _, existed := oldByName[name]; existed {
			continue
		}
		nf := newByName[name]
		if nf.Required {
			changes = append(changes, Change{
				Kind:     ChangeAddedRequiredField,
				Field:    name,
				Detail:   fmt.Sprintf("required field %s added", name),
				Breaking: true, // 旧数据缺少该必填字段
			})
		} else {
			changes = append(changes, Change{
				Kind:     ChangeAddedOptionalField,
				Field:    name,
				Detail:   fmt.Sprintf("optional field %s added", name),
				Breaking: false,
			})
		}
	}

	if !sameStringSet(oldC.KeyFields, newC.KeyFields) {
		changes = append(changes, Change{
			Kind:     ChangeKeyFieldsChanged,
			Detail:   fmt.Sprintf("key groups changed from %v to %v", oldC.KeyFields, newC.KeyFields),
			Breaking: true,
		})
	}
	return changes
}

func diffField(of, nf model.FieldSpec) []Change {
	var out []Change
	if of.Type != nf.Type {
		out = append(out, Change{
			Kind:     ChangeTypeChanged,
			Field:    of.Name,
			Detail:   fmt.Sprintf("type changed %s -> %s", of.Type, nf.Type),
			Breaking: true,
		})
	}
	if !of.Required && nf.Required {
		out = append(out, Change{
			Kind:     ChangeOptionalToRequired,
			Field:    of.Name,
			Detail:   "optional field became required",
			Breaking: true,
		})
	}
	if of.Required && !nf.Required {
		out = append(out, Change{
			Kind:     ChangeRequiredToOptional,
			Field:    of.Name,
			Detail:   "required field became optional",
			Breaking: false,
		})
	}
	// 数值约束：收紧为破坏性，放宽为兼容。
	if tightened, loosened := constraintChanged(of, nf); tightened || loosened {
		out = append(out, Change{
			Kind:     boolKind(tightened, ChangeConstraintTightened, ChangeConstraintLoosened),
			Field:    of.Name,
			Detail:   fmt.Sprintf("range changed from [%s,%s] to [%s,%s]", pfmt(of.Min), pfmt(of.Max), pfmt(nf.Min), pfmt(nf.Max)),
			Breaking: tightened,
		})
	}
	return out
}

func constraintChanged(of, nf model.FieldSpec) (tightened, loosened bool) {
	if of.Type != model.TypeInt && of.Type != model.TypeFloat {
		return false, false
	}
	// 新下界提高 或 新上界降低 => 收紧；反之放宽。
	switch {
	case nf.Min != nil && (of.Min == nil || *nf.Min > *of.Min):
		tightened = true
	case of.Min != nil && (nf.Min == nil || *nf.Min < *of.Min):
		loosened = true
	}
	switch {
	case nf.Max != nil && (of.Max == nil || *nf.Max < *of.Max):
		tightened = true
	case of.Max != nil && (nf.Max == nil || *nf.Max > *of.Max):
		loosened = true
	}
	return tightened, loosened
}

func boolKind(cond bool, a, b ChangeKind) ChangeKind {
	if cond {
		return a
	}
	return b
}

func pfmt(p *float64) string {
	if p == nil {
		return "-inf/+inf"
	}
	return fmt.Sprintf("%v", *p)
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, x := range a {
		seen[x]++
	}
	for _, x := range b {
		seen[x]--
		if seen[x] < 0 {
			return false
		}
	}
	return true
}
