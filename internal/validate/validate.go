// Package validate 按数据契约校验批次，违规记录归入可区分原因；
// 自定义规则在独立 goroutine 中执行，panic/超时被隔离。
package validate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// CustomRule 是调用方提供的记录级自定义校验。
// 返回非空 reason 表示该记录违规；实现可能 panic 或超时。
type CustomRule func(rec model.Record) (reason model.Reason, detail string)

// Options 控制校验器行为。
type Options struct {
	// CustomRules 按顺序作用于每条通过内建校验的记录。
	CustomRules []CustomRule
	// RuleTimeout 单条记录全部自定义规则的执行超时；0 表示不限时。
	RuleTimeout time.Duration
}

// Validator 按单个契约版本校验批次。
type Validator interface {
	// Validate 对批次逐条判定，不修改入参。
	Validate(c model.Contract, b model.Batch, opts Options) []model.RecordResult
}

type defaultValidator struct{}

// NewValidator 创建默认校验器。
func NewValidator() Validator { return &defaultValidator{} }

// Validate 对每条记录执行：缺失 -> 类型 -> 约束 -> 键组重复 -> 自定义规则。
// 一条记录可带多个违规，但原因码彼此可区分；结果按记录下标稳定排序。
func (v *defaultValidator) Validate(c model.Contract, b model.Batch, opts Options) []model.RecordResult {
	results := make([]model.RecordResult, len(b.Records))
	specs := make(map[string]model.FieldSpec, len(c.Fields))
	for _, fs := range c.Fields {
		specs[fs.Name] = fs
	}
	// 键组 -> 首次出现下标。
	seenKeys := map[string]int{}

	for i, rec := range b.Records {
		rr := model.RecordResult{Index: i}
		if rec == nil {
			rr.Violations = append(rr.Violations, model.Violation{
				Reason: model.ReasonMalformed, Detail: "record is null or not an object",
			})
			results[i] = rr
			continue
		}
		// 内建字段级校验（按契约字段声明顺序输出，保证可复现）。
		for _, fs := range c.Fields {
			val, present := rec[fs.Name]
			if !present {
				if fs.Required {
					rr.Violations = append(rr.Violations, model.Violation{
						Field: fs.Name, Reason: model.ReasonMissingField,
						Detail: fmt.Sprintf("required field %s missing", fs.Name),
					})
				}
				continue
			}
			if num, ok := numberCheck(fs, val, &rr); ok {
				rangeCheck(fs, num, &rr)
			}
		}
		// 键组重复：只要键字段本身合法即参与去重，非键字段的其它违规
		// 不应掩盖键组重复这一独立原因。
		if len(c.KeyFields) > 0 && keyFieldsClean(c.KeyFields, rr.Violations) {
			key := compositeKey(rec, c.KeyFields)
			if first, dup := seenKeys[key]; dup {
				rr.Violations = append(rr.Violations, model.Violation{
					Field:  strings.Join(c.KeyFields, ","),
					Reason: model.ReasonKeyDuplicate,
					Detail: fmt.Sprintf("key group %s duplicates record at index %d", key, first),
				})
			} else {
				seenKeys[key] = i
			}
		}
		// 自定义规则只作用于内建校验完全通过的记录，保证失败域隔离清晰。
		if len(rr.Violations) == 0 && len(opts.CustomRules) > 0 {
			rr.Violations = append(rr.Violations, runCustomRules(rec, opts)...)
		}
		rr.Valid = len(rr.Violations) == 0
		results[i] = rr
	}
	return results
}

// numberCheck 检查存在性之外的类型/null 合法性，返回可用于范围检查的数值。
// ok 为 false 表示已记录类型违规或值缺失/不可数值化。
func numberCheck(fs model.FieldSpec, val any, rr *model.RecordResult) (float64, bool) {
	if val == nil {
		if !fs.Nullable {
			rr.Violations = append(rr.Violations, model.Violation{
				Field: fs.Name, Reason: model.ReasonNullNotAllowed,
				Detail: fmt.Sprintf("field %s is null but not nullable", fs.Name),
			})
		}
		return 0, false
	}
	switch fs.Type {
	case model.TypeInt:
		n, ok := asInt(val)
		if !ok {
			addTypeViolation(fs, val, rr)
			return 0, false
		}
		return float64(n), true
	case model.TypeFloat:
		n, ok := asFloat(val)
		if !ok {
			addTypeViolation(fs, val, rr)
			return 0, false
		}
		return n, true
	case model.TypeString:
		if _, ok := val.(string); !ok {
			addTypeViolation(fs, val, rr)
		}
	case model.TypeBool:
		if _, ok := val.(bool); !ok {
			addTypeViolation(fs, val, rr)
		}
	}
	return 0, false
}

func addTypeViolation(fs model.FieldSpec, val any, rr *model.RecordResult) {
	rr.Violations = append(rr.Violations, model.Violation{
		Field:  fs.Name,
		Reason: model.ReasonTypeConflict,
		Detail: fmt.Sprintf("field %s expects %s but got %T", fs.Name, fs.Type, val),
	})
}

func rangeCheck(fs model.FieldSpec, num float64, rr *model.RecordResult) {
	if fs.Min != nil && num < *fs.Min {
		rr.Violations = append(rr.Violations, model.Violation{
			Field: fs.Name, Reason: model.ReasonConstraint,
			Detail: fmt.Sprintf("value %v below min %v", num, *fs.Min),
		})
	}
	if fs.Max != nil && num > *fs.Max {
		rr.Violations = append(rr.Violations, model.Violation{
			Field: fs.Name, Reason: model.ReasonConstraint,
			Detail: fmt.Sprintf("value %v above max %v", num, *fs.Max),
		})
	}
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	case float32:
		if f := float64(x); f == float64(int64(f)) {
			return int64(f), true
		}
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
	case jsonNumber:
		if n, err := x.Int64(); err == nil {
			return n, true
		}
	}
	return 0, false
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case jsonNumber:
		if f, err := x.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// jsonNumber 避免强制导入 encoding/json；strconv.NumError 等实现了该接口。
// 实际项目中 json.Number 自动满足。
type jsonNumber interface {
	Int64() (int64, error)
	Float64() (float64, error)
}

func compositeKey(rec model.Record, keys []string) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%v", rec[k])
	}
	return strings.Join(parts, "\x00")
}

// keyFieldsClean 判断现有违规是否都不涉及键字段（键字段缺失/类型错误时
// 无法可靠计算键组，此时不报重复）。
func keyFieldsClean(keys []string, violations []model.Violation) bool {
	bad := map[string]bool{}
	for _, v := range violations {
		bad[v.Field] = true
	}
	for _, k := range keys {
		if bad[k] {
			return false
		}
	}
	return true
}

// runCustomRules 顺序执行自定义规则；每条记录的执行放在独立 goroutine 中，
// panic 被 recover 转换为 custom_rule_panic，超时转换为 custom_rule_timeout，
// 均不会影响引擎其它批次。
func runCustomRules(rec model.Record, opts Options) []model.Violation {
	type ruleOutcome struct {
		reason model.Reason
		detail string
	}
	out := make(chan ruleOutcome, 1)
	go func() {
		var first ruleOutcome
		func() {
			defer func() {
				if r := recover(); r != nil {
					first = ruleOutcome{reason: model.ReasonCustomPanic,
						detail: fmt.Sprintf("custom rule panicked: %v", r)}
				}
			}()
			for _, rule := range opts.CustomRules {
				reason, detail := safeRule(rule, rec)
				if reason != model.ReasonNone {
					first = ruleOutcome{reason: reason, detail: detail}
					return
				}
			}
		}()
		out <- first
	}()

	var res ruleOutcome
	if opts.RuleTimeout > 0 {
		timer := time.NewTimer(opts.RuleTimeout)
		defer timer.Stop()
		select {
		case res = <-out:
		case <-timer.C:
			res = ruleOutcome{reason: model.ReasonCustomTimeout,
				detail: "custom rules exceeded " + strconv.FormatInt(opts.RuleTimeout.Milliseconds(), 10) + "ms"}
		}
	} else {
		res = <-out
	}
	if res.reason == model.ReasonNone {
		return nil
	}
	return []model.Violation{{Reason: res.reason, Detail: res.detail}}
}

// safeRule 包裹单条规则，确保 panic 不会终止其它规则的整体隔离语义。
func safeRule(rule CustomRule, rec model.Record) (reason model.Reason, detail string) {
	defer func() {
		if r := recover(); r != nil {
			reason = model.ReasonCustomPanic
			detail = fmt.Sprintf("custom rule panicked: %v", r)
		}
	}()
	r, d := rule(rec)
	return r, d
}
