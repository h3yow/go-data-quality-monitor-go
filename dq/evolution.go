package dq

import (
	"fmt"
	"strings"
)

// ChangeKind 标识单项契约变化的类别。
type ChangeKind string

const (
	ChangeFieldAdded          ChangeKind = "field_added"
	ChangeFieldRemoved        ChangeKind = "field_removed"
	ChangeTypeChanged         ChangeKind = "type_changed"
	ChangeRequiredTightened   ChangeKind = "required_tightened" // 可选 -> 必填(收紧)
	ChangeRequiredRelaxed     ChangeKind = "required_relaxed"   // 必填 -> 可选(放宽)
	ChangeConstraintTightened ChangeKind = "constraint_tightened"
	ChangeConstraintRelaxed   ChangeKind = "constraint_relaxed"
	ChangeKeyGroupChanged     ChangeKind = "key_group_changed"
)

// Change 描述从旧契约演进到新契约时的单项变化及其兼容性结论。
// 收紧类变化(让原本合法的数据可能突然不合法)一律视为不兼容；
// 放宽类变化(扩大合法数据集合)视为兼容。
type Change struct {
	Kind       ChangeKind
	Field      string // 键组变化时为空
	Compatible bool
	Detail     string
}

func (c Change) String() string {
	flag := "compatible"
	if !c.Compatible {
		flag = "INCOMPATIBLE"
	}
	if c.Field != "" {
		return fmt.Sprintf("%s field %q: %s [%s]", c.Kind, c.Field, c.Detail, flag)
	}
	return fmt.Sprintf("%s: %s [%s]", c.Kind, c.Detail, flag)
}

// EvolutionAction 表示一次演进尝试最终采取的处置。
type EvolutionAction string

const (
	EvolutionApplied  EvolutionAction = "applied"
	EvolutionRejected EvolutionAction = "rejected"
	EvolutionBypassed EvolutionAction = "bypassed"
)

// EvolutionReport 是一次契约演进的可解释结论：
// 每项变化分别是什么、是否兼容、原因，以及策略最终采取的处置。
type EvolutionReport struct {
	FromVersion int
	ToVersion   int
	Action      EvolutionAction
	Changes     []Change
	Warnings    []string
	Err         error
}

// Compatible 报告本次演进是否全部为兼容变化。
func (r EvolutionReport) Compatible() bool {
	for _, c := range r.Changes {
		if !c.Compatible {
			return false
		}
	}
	return true
}

// Incompatible 返回全部不兼容变化的副本。
func (r EvolutionReport) Incompatible() []Change {
	var out []Change
	for _, c := range r.Changes {
		if !c.Compatible {
			out = append(out, c)
		}
	}
	return out
}

func (r EvolutionReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "evolution v%d -> v%d: action=%s", r.FromVersion, r.ToVersion, r.Action)
	if r.Err != nil {
		fmt.Fprintf(&b, " err=%v", r.Err)
	}
	b.WriteString("\n")
	for _, c := range r.Changes {
		fmt.Fprintf(&b, "  - %s\n", c)
	}
	return strings.TrimRight(b.String(), "\n")
}

// AnalyzeEvolution 逐项分析从 old 到 next 的契约变化。
// 输出顺序确定：先按新契约字段顺序处理新增/共有字段，
// 再按旧契约字段顺序报告删除字段，最后是键组变化。
//
// 兼容性判定原则：只有"扩大合法数据集合"的变化才算兼容，
// 任何可能让原本合法数据变得不合法的收紧都算不兼容：
//   - 新增可选字段 / 删除字段：删除字段不会让旧合法数据无法通过(旧字段被忽略)，
//     但既有语义将删除字段视为不兼容，保持该策略；
//   - 字段类型变化：不兼容；
//   - 必填 -> 可选：兼容(放宽)；可选 -> 必填：不兼容(收紧)；
//   - 约束(min/max/maxLen)收窄：不兼容；放宽或解除：兼容；
//   - 键组变化：不兼容。
func AnalyzeEvolution(old, next Contract) []Change {
	var changes []Change
	oldIdx := old.fieldIndex()
	nextIdx := next.fieldIndex()

	for _, nf := range next.Fields {
		of, existed := oldIdx[nf.Name]
		if !existed {
			if nf.Required {
				changes = append(changes, Change{
					Kind: ChangeFieldAdded, Field: nf.Name, Compatible: false,
					Detail: "new required field: previously valid records now miss it",
				})
			} else {
				changes = append(changes, Change{
					Kind: ChangeFieldAdded, Field: nf.Name, Compatible: true,
					Detail: "new optional field",
				})
			}
			continue
		}
		if of.Type != nf.Type {
			changes = append(changes, Change{
				Kind: ChangeTypeChanged, Field: nf.Name, Compatible: false,
				Detail: fmt.Sprintf("type changed %s -> %s", of.Type, nf.Type),
			})
		}
		if !of.Required && nf.Required {
			changes = append(changes, Change{
				Kind: ChangeRequiredTightened, Field: nf.Name, Compatible: false,
				Detail: "nullable/optional field became required",
			})
		} else if of.Required && !nf.Required {
			changes = append(changes, Change{
				Kind: ChangeRequiredRelaxed, Field: nf.Name, Compatible: true,
				Detail: "required field became optional",
			})
		}
		changes = append(changes, analyzeConstraint(of, nf)...)
	}

	for _, of := range old.Fields {
		if _, ok := nextIdx[of.Name]; !ok {
			changes = append(changes, Change{
				Kind: ChangeFieldRemoved, Field: of.Name, Compatible: false,
				Detail: "field removed",
			})
		}
	}

	if !equalStrings(old.KeyGroup, next.KeyGroup) {
		changes = append(changes, Change{
			Kind:       ChangeKeyGroupChanged,
			Detail:     fmt.Sprintf("key group %v -> %v", old.KeyGroup, next.KeyGroup),
			Compatible: false,
		})
	}
	return changes
}

// analyzeConstraint 比较共有字段的数值上下界与字符串长度约束。
// 每个维度独立报告，便于解释"收窄在哪"。同时出现放宽与收紧
// (如下界放宽、上界收窄)时，整体含收紧项，即不兼容。
func analyzeConstraint(of, nf FieldSpec) []Change {
	var changes []Change
	oc, nc := of.Constraint, nf.Constraint

	type bound struct {
		name          string
		oldV, newV    *float64
		tightenedWhen func(o, n float64) bool
		introducedMsg func(n float64) string
	}
	numBounds := []bound{
		{
			name: "min", oldV: floatMin(oc), newV: floatMin(nc),
			tightenedWhen: func(o, n float64) bool { return n > o },
			introducedMsg: func(n float64) string { return fmt.Sprintf("min bound introduced: values below %v now rejected", n) },
		},
		{
			name: "max", oldV: floatMax(oc), newV: floatMax(nc),
			tightenedWhen: func(o, n float64) bool { return n < o },
			introducedMsg: func(n float64) string { return fmt.Sprintf("max bound introduced: values above %v now rejected", n) },
		},
	}
	for _, bd := range numBounds {
		switch {
		case bd.oldV == nil && bd.newV == nil:
			// 无变化
		case bd.oldV == nil:
			// 原本无界，现在加界：收窄。
			changes = append(changes, Change{
				Kind: ChangeConstraintTightened, Field: nf.Name, Compatible: false,
				Detail: bd.introducedMsg(*bd.newV),
			})
		case bd.newV == nil:
			// 解除界限：放宽。
			changes = append(changes, Change{
				Kind: ChangeConstraintRelaxed, Field: nf.Name, Compatible: true,
				Detail: fmt.Sprintf("%s bound %v removed", bd.name, *bd.oldV),
			})
		case *bd.oldV == *bd.newV:
			// 无变化
		case bd.tightenedWhen(*bd.oldV, *bd.newV):
			changes = append(changes, Change{
				Kind: ChangeConstraintTightened, Field: nf.Name, Compatible: false,
				Detail: fmt.Sprintf("%s narrowed %v -> %v", bd.name, *bd.oldV, *bd.newV),
			})
		default:
			changes = append(changes, Change{
				Kind: ChangeConstraintRelaxed, Field: nf.Name, Compatible: true,
				Detail: fmt.Sprintf("%s widened %v -> %v", bd.name, *bd.oldV, *bd.newV),
			})
		}
	}

	// MaxLen:仅对字符串字段有意义，但对数值字段误配 MaxLen 时原样比较也安全。
	oldLen := intMaxLen(oc)
	newLen := intMaxLen(nc)
	switch {
	case oldLen == nil && newLen == nil:
	case oldLen == nil:
		changes = append(changes, Change{
			Kind: ChangeConstraintTightened, Field: nf.Name, Compatible: false,
			Detail: fmt.Sprintf("max length introduced: %d rejects previously valid longer values", *newLen),
		})
	case newLen == nil:
		changes = append(changes, Change{
			Kind: ChangeConstraintRelaxed, Field: nf.Name, Compatible: true,
			Detail: fmt.Sprintf("max length %d removed", *oldLen),
		})
	case *oldLen == *newLen:
	case *newLen < *oldLen:
		changes = append(changes, Change{
			Kind: ChangeConstraintTightened, Field: nf.Name, Compatible: false,
			Detail: fmt.Sprintf("max length narrowed %d -> %d", *oldLen, *newLen),
		})
	default:
		changes = append(changes, Change{
			Kind: ChangeConstraintRelaxed, Field: nf.Name, Compatible: true,
			Detail: fmt.Sprintf("max length widened %d -> %d", *oldLen, *newLen),
		})
	}
	return changes
}

func floatMin(c *Constraint) *float64 {
	if c == nil {
		return nil
	}
	return c.Min
}

func floatMax(c *Constraint) *float64 {
	if c == nil {
		return nil
	}
	return c.Max
}

func intMaxLen(c *Constraint) *int {
	if c == nil {
		return nil
	}
	return c.MaxLen
}

// EvolveDetailed 尝试演进契约并返回可解释报告。
// 处置与 CompatMode 一致：
//   - 版本未严格递增：rejected(Err 非空，契约不变，不产生 Changes)；
//   - 全部兼容：applied，契约切换到新版本；
//   - 存在收紧项 + CompatReject：rejected(Err 非空，契约不变)；
//   - 存在收紧项 + CompatBypass：bypassed(契约不变，记录告警)；
//   - 存在收紧项 + CompatWarn：applied(强制应用，记录告警)。
func (e *Engine) EvolveDetailed(next Contract) EvolutionReport {
	e.mu.Lock()
	defer e.mu.Unlock()

	report := EvolutionReport{FromVersion: e.contract.Version, ToVersion: next.Version}
	if next.Version <= e.contract.Version {
		report.Action = EvolutionRejected
		report.Err = fmt.Errorf("dq: contract version must increase (current %d, got %d)",
			e.contract.Version, next.Version)
		return report
	}

	report.Changes = AnalyzeEvolution(e.contract, next)
	incompat := report.Incompatible()
	if len(incompat) > 0 {
		switch e.compat {
		case CompatReject:
			report.Action = EvolutionRejected
			report.Err = fmt.Errorf("dq: incompatible contract evolution rejected: %s",
				formatChanges(incompat))
			return report
		case CompatBypass:
			report.Action = EvolutionBypassed
			w := fmt.Sprintf("contract v%d bypassed due to incompatibility: %s",
				next.Version, formatChanges(incompat))
			e.warnings = append(e.warnings, w)
			report.Warnings = append(report.Warnings, w)
			return report
		case CompatWarn:
			w := fmt.Sprintf("contract v%d applied despite incompatibility: %s",
				next.Version, formatChanges(incompat))
			e.warnings = append(e.warnings, w)
			report.Warnings = append(report.Warnings, w)
		}
	}

	e.contract = cloneContract(next)
	report.Action = EvolutionApplied
	return report
}

func formatChanges(cs []Change) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = c.String()
	}
	return "[" + strings.Join(parts, "; ") + "]"
}

// cloneContract 深拷贝契约(切片与指针约束)，避免调用方在提交后
// 修改底层切片/约束导致引擎状态被外部篡改。
func cloneContract(c Contract) Contract {
	out := c
	out.Fields = append([]FieldSpec(nil), c.Fields...)
	for i := range out.Fields {
		if c.Fields[i].Constraint != nil {
			cp := *c.Fields[i].Constraint
			out.Fields[i].Constraint = &cp
		}
	}
	out.KeyGroup = append([]string(nil), c.KeyGroup...)
	return out
}
