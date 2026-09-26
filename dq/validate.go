package dq

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CustomValidator 是调用方提供的自定义校验逻辑。
// 返回 nil 表示通过；返回非 nil error 表示记录不合法。
type CustomValidator func(ctx context.Context, r Record) error

// Validator 按某一版本契约校验批次。
// 同一契约版本对同一输入的校验结论可复现(确定性)。
type Validator struct {
	contract Contract
	custom   []CustomValidator
	timeout  time.Duration
}

// NewValidator 创建基于指定契约版本的校验器。
// timeout 为单条记录上全部自定义校验的总时限；<=0 表示不限制。
func NewValidator(c Contract, custom []CustomValidator, timeout time.Duration) *Validator {
	return &Validator{contract: c, custom: custom, timeout: timeout}
}

// Contract 返回校验器使用的契约。
func (v *Validator) Contract() Contract { return v.contract }

// Validate 校验整个批次，返回可复现的校验结论。
// 不合法记录不会被静默跳过，而是携带可区分原因进入 Violations。
func (v *Validator) Validate(b Batch) ValidationResult {
	res := ValidationResult{BatchID: b.ID, ContractVersion: v.contract.Version}
	seen := make(map[string]int) // 键组 -> 首次出现的行号
	for i, r := range b.Records {
		viol := v.validateRecord(b.ID, i, r, seen)
		if len(viol) == 0 {
			res.Accepted = append(res.Accepted, r)
		} else {
			res.Violations = append(res.Violations, viol...)
		}
	}
	return res
}

// validateRecord 校验单条记录，返回其全部违规(可能多条)。
func (v *Validator) validateRecord(batchID string, row int, r Record, seen map[string]int) []Violation {
	var out []Violation
	add := func(reason Reason, field, detail string) {
		out = append(out, Violation{BatchID: batchID, Row: row, Reason: reason, Field: field, Detail: detail})
	}

	// 1. 字段缺失与类型冲突、约束越界：按契约字段声明顺序遍历，保证确定性。
	for _, f := range v.contract.Fields {
		val, ok := r[f.Name]
		if !ok || val == nil {
			if f.Required {
				add(ReasonFieldMissing, f.Name, "required field missing or null")
			}
			continue
		}
		if !typeMatches(val, f.Type) {
			add(ReasonTypeConflict, f.Name,
				fmt.Sprintf("expected %s, got %T", f.Type, val))
			continue // 类型不符时不继续做约束检查
		}
		if f.Constraint != nil {
			if detail, breached := checkConstraint(val, f.Constraint); breached {
				add(ReasonConstraintBreach, f.Name, detail)
			}
		}
	}

	// 2. 键组重复：批次内首次出现接受，后续出现判重。
	if len(v.contract.KeyGroup) > 0 {
		key, ok := keyOf(r, v.contract.KeyGroup)
		if ok {
			if first, dup := seen[key]; dup {
				add(ReasonDuplicateKey, strings.Join(v.contract.KeyGroup, ","),
					fmt.Sprintf("duplicate of row %d (key %s)", first, key))
			} else {
				seen[key] = row
			}
		}
	}

	// 3. 自定义校验：panic 与超时隔离，不中断其他检查。
	if len(v.custom) > 0 {
		if reason, detail, failed := runCustomIsolated(v.custom, v.timeout, r); failed {
			add(reason, "", detail)
		}
	}
	return out
}

// typeMatches 判断值是否与契约字段类型匹配。
func typeMatches(val any, t FieldType) bool {
	switch t {
	case TypeString:
		_, ok := val.(string)
		return ok
	case TypeBool:
		_, ok := val.(bool)
		return ok
	case TypeInt:
		switch val.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	case TypeFloat:
		switch val.(type) {
		case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		}
		return false
	}
	return false
}

// toFloat 将数值值转为 float64；非数值返回 false。
func toFloat(val any) (float64, bool) {
	switch n := val.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// checkConstraint 检查约束，返回违规描述与是否越界。
func checkConstraint(val any, c *Constraint) (string, bool) {
	if f, ok := toFloat(val); ok {
		if c.Min != nil && f < *c.Min {
			return fmt.Sprintf("value %v below min %v", f, *c.Min), true
		}
		if c.Max != nil && f > *c.Max {
			return fmt.Sprintf("value %v above max %v", f, *c.Max), true
		}
	}
	if s, ok := val.(string); ok && c.MaxLen != nil && len(s) > *c.MaxLen {
		return fmt.Sprintf("length %d exceeds max %d", len(s), *c.MaxLen), true
	}
	return "", false
}

// keyOf 从记录中提取键组的规范化字符串表示。
// 任一键字段缺失或类型无法规范化时返回 false(该记录不参与判重，
// 其缺失问题已由字段缺失检查覆盖)。
func keyOf(r Record, group []string) (string, bool) {
	parts := make([]string, 0, len(group))
	for _, name := range group {
		val, ok := r[name]
		if !ok || val == nil {
			return "", false
		}
		parts = append(parts, fmt.Sprintf("%#v", val))
	}
	return strings.Join(parts, "\x1f"), true
}

// runCustomIsolated 在隔离环境中运行全部自定义校验器：
//   - panic 被恢复并转换为 ReasonValidatorFault；
//   - 整体超时(>0 时)被转换为 ReasonValidatorFault；
//   - 普通 error 转换为 ReasonCustomCheck。
//
// 任何故障都不会向调用方泄漏 panic，也不会阻塞其他校验。
func runCustomIsolated(custom []CustomValidator, timeout time.Duration, r Record) (reason Reason, detail string, failed bool) {
	type result struct {
		reason Reason
		detail string
		failed bool
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- result{ReasonValidatorFault, fmt.Sprintf("custom validator panic: %v", p), true}
			}
		}()
		for i, cv := range custom {
			if err := cv(context.Background(), r); err != nil {
				done <- result{ReasonCustomCheck, fmt.Sprintf("custom validator %d: %v", i, err), true}
				return
			}
		}
		done <- result{}
	}()

	if timeout <= 0 {
		res := <-done
		return res.reason, res.detail, res.failed
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-done:
		return res.reason, res.detail, res.failed
	case <-timer.C:
		return ReasonValidatorFault, fmt.Sprintf("custom validators timed out after %s", timeout), true
	}
}

// SortViolations 按(行号, 原因, 字段)稳定排序违规，便于输出可复现。
func SortViolations(vs []Violation) {
	sort.SliceStable(vs, func(i, j int) bool {
		if vs[i].Row != vs[j].Row {
			return vs[i].Row < vs[j].Row
		}
		if vs[i].Reason != vs[j].Reason {
			return vs[i].Reason < vs[j].Reason
		}
		return vs[i].Field < vs[j].Field
	})
}
