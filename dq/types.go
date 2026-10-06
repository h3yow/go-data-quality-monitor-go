// Package dq 提供可嵌入的数据质量校验与增量监控引擎。
package dq

import "time"

// Reason 表示记录被判定为不合法的可区分原因。
type Reason string

const (
	ReasonFieldMissing     Reason = "field_missing"     // 必需字段缺失
	ReasonTypeConflict     Reason = "type_conflict"     // 字段类型与契约冲突
	ReasonConstraintBreach Reason = "constraint_breach" // 约束越界
	ReasonDuplicateKey     Reason = "duplicate_key"     // 键组重复
	ReasonSchemaRejected   Reason = "schema_rejected"   // 结构不兼容被拒绝
	ReasonValidatorFault   Reason = "validator_fault"   // 自定义校验逻辑故障(panic/超时)
	ReasonCustomCheck      Reason = "custom_check"      // 自定义校验逻辑判定不合法
)

// FieldType 是契约支持的字段类型。
type FieldType string

const (
	TypeString FieldType = "string"
	TypeInt    FieldType = "int"
	TypeFloat  FieldType = "float"
	TypeBool   FieldType = "bool"
)

// Record 是一条待校验的数据记录。
type Record map[string]any

// Batch 是一批输入数据，携带单调批次标识与事件时间。
type Batch struct {
	ID        string    // 批次标识，用于去重与日志
	Seq       uint64    // 批次序号，用于乱序检测
	Records   []Record  // 批次内记录
	EventTime time.Time // 批次事件时间，用于新鲜度
}

// Violation 描述一条记录的不合法判定。
type Violation struct {
	BatchID string
	Row     int
	Reason  Reason
	Field   string
	Detail  string
}

// ValidationResult 是单批次校验结论。
type ValidationResult struct {
	BatchID         string
	ContractVersion int
	Accepted        []Record
	Violations      []Violation
}
