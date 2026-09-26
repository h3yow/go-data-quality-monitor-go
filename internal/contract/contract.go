// Package contract 实现可版本化数据契约、契约演进差异与结构兼容策略。
package contract

import "github.com/h3yow/go-data-quality-monitor-go/internal/model"

// CompatMode 定义结构兼容性变化时的处理模式。
type CompatMode string

const (
	// ModeReject 拒绝该批次，不更新水位与基线。
	ModeReject CompatMode = "reject"
	// ModeSideways 旁路接收：单独计数，不更新主基线。
	ModeSideways CompatMode = "sideways"
	// ModeAlert 告警但正常接收，按明确规则更新基线。
	ModeAlert CompatMode = "alert"
)

// ChangeKind 描述契约演进的变更类别。
type ChangeKind string

const (
	ChangeAddedRequiredField  ChangeKind = "added_required_field"
	ChangeAddedOptionalField  ChangeKind = "added_optional_field"
	ChangeRemovedField        ChangeKind = "removed_field"
	ChangeTypeChanged         ChangeKind = "type_changed"
	ChangeRequiredToOptional  ChangeKind = "required_to_optional"
	ChangeOptionalToRequired  ChangeKind = "optional_to_required"
	ChangeConstraintTightened ChangeKind = "constraint_tightened"
	ChangeConstraintLoosened  ChangeKind = "constraint_loosened"
	ChangeKeyFieldsChanged    ChangeKind = "key_fields_changed"
)

// Change 描述一处契约差异。
type Change struct {
	Kind   ChangeKind `json:"kind"`
	Field  string     `json:"field,omitempty"`
	Detail string     `json:"detail,omitempty"`
	// Breaking 表示该变化是否可能破坏既有数据。
	Breaking bool `json:"breaking"`
}

// Policy 定义结构兼容性变化的处理策略。
type Policy struct {
	// OnBreaking 出现破坏性变化时的处理模式（默认 reject）。
	OnBreaking CompatMode
	// OnCompatible 向后兼容变化（如新增可选字段）时的处理模式（默认 alert）。
	OnCompatible CompatMode
	// OnDowngrade 出现比当前生效版本更旧的契约时的处理模式（默认 alert）。
	OnDowngrade CompatMode
}

// Registry 保存一个契约名下所有已注册版本，按语义化版本排序。
type Registry interface {
	Register(c model.Contract) error
	Get(name, version string) (model.Contract, bool)
	Active(name string) (model.Contract, bool)
	Versions(name string) []string
}
