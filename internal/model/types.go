// Package model 定义数据质量引擎的核心数据类型。
package model

// Record 是一条记录的通用表示（字段名 -> 值）。
type Record map[string]any

// FieldType 声明契约字段类型。
type FieldType string

// 支持的字段类型。
const (
	TypeString  FieldType = "string"
	TypeInt     FieldType = "int"
	TypeFloat   FieldType = "float"
	TypeBool    FieldType = "bool"
	TypeUnknown FieldType = "unknown"
)

// FieldSpec 描述单个字段的契约规格。
type FieldSpec struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	// Nullable 为 false 时，显式的 null 值视为类型冲突。
	Nullable bool `json:"nullable"`
	// 仅对数值类型生效的取值范围约束（闭区间）。
	Min *float64 `json:"min,omitempty"`
	Max *float64 `json:"max,omitempty"`
}

// Contract 是可版本化的数据契约。
type Contract struct {
	Name       string      `json:"name"`
	Version    string      `json:"version"`
	Fields     []FieldSpec `json:"fields"`
	KeyFields  []string    `json:"key_fields"`
	Deprecated bool        `json:"deprecated"`
}

// Batch 表示一次提交的批次（带业务事件时间，用于乱序判定）。
type Batch struct {
	BatchID   string   `json:"batch_id"`
	ContractV string   `json:"contract_version"`
	Records   []Record `json:"records"`
	// EventTimeMillis 批次携带的业务事件时间（毫秒 epoch）。
	EventTimeMillis int64 `json:"event_time_millis"`
}

// Reason 是记录级或批次级的不可通过原因码。
type Reason string

// 记录级原因码（相互可区分）。
const (
	ReasonNone           Reason = ""
	ReasonMalformed      Reason = "malformed_record" // 记录无法解析/非对象
	ReasonMissingField   Reason = "missing_field"
	ReasonTypeConflict   Reason = "type_conflict"
	ReasonNullNotAllowed Reason = "null_not_allowed"
	ReasonConstraint     Reason = "constraint_violation"
	ReasonKeyDuplicate   Reason = "duplicate_key_group"
	ReasonCustomPanic    Reason = "custom_rule_panic"
	ReasonCustomTimeout  Reason = "custom_rule_timeout"
	ReasonCustomReject   Reason = "custom_rule_reject"
)

// 批次级原因码。
const (
	BatchReasonNone             Reason = ""
	BatchReasonUnknownContract  Reason = "unknown_contract"
	BatchReasonDeprecated       Reason = "contract_deprecated"
	BatchReasonContractRejected Reason = "contract_change_rejected"
)

// Violation 描述单条记录的一次违规。
type Violation struct {
	Field  string `json:"field,omitempty"`
	Reason Reason `json:"reason"`
	Detail string `json:"detail,omitempty"`
}

// RecordResult 是单条记录的判定结果。
type RecordResult struct {
	Index      int         `json:"index"`
	Valid      bool        `json:"valid"`
	Violations []Violation `json:"violations,omitempty"`
}

// Outcome 是批次结论。
type Outcome string

const (
	// OutcomeAccepted 批次通过全部校验并已计入监控指标。
	OutcomeAccepted Outcome = "accepted"
	// OutcomeRejected 批次存在非法记录或被契约策略拒绝，不更新基线/水位。
	OutcomeRejected Outcome = "rejected"
	// OutcomeDuplicate 重复批次：返回首次结论的快照，不重复计数。
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeSideways 结构性不兼容变化，按策略旁路：单独计数，不进主基线。
	OutcomeSideways Outcome = "sideways"
)

// Alert 是引擎产生的告警。
type Alert struct {
	BatchID     string `json:"batch_id"`
	ContractV   string `json:"contract_version"`
	Kind        string `json:"kind"`
	Detail      string `json:"detail,omitempty"`
	EventTimeMs int64  `json:"event_time_ms"`
	SeenTimeMs  int64  `json:"seen_time_ms"`
}

// BatchResult 是一次批次提交的完整、可解释结论。
type BatchResult struct {
	BatchID       string         `json:"batch_id"`
	ContractName  string         `json:"contract_name"`
	ContractV     string         `json:"contract_version"`
	Outcome       Outcome        `json:"outcome"`
	Reasons       []Reason       `json:"reasons,omitempty"`
	ReasonCounts  map[Reason]int `json:"reason_counts,omitempty"`
	RecordResults []RecordResult `json:"record_results,omitempty"`
	Alerts        []Alert        `json:"alerts,omitempty"`
	Total         int            `json:"total"`
	ValidCount    int            `json:"valid_count"`
	InvalidCount  int            `json:"invalid_count"`
	MetricsAfter  *Snapshot      `json:"metrics_after,omitempty"`
	EventTimeMs   int64          `json:"event_time_ms"`
	ProcessedMs   int64          `json:"processed_ms"`
}

// FieldDist 记录单个数值字段的分布基线（Welford 增量统计）。
type FieldDist struct {
	Field    string  `json:"field"`
	Count    int64   `json:"count"`
	Mean     float64 `json:"mean"`
	M2       float64 `json:"m2"` // Welford M2
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	FrozenAt int64   `json:"frozen_at"`
}

// StdDev 返回样本标准差；样本不足两个时返回 0。
// 用牛顿迭代求平方根，使 model 包保持零导入依赖。
func (d FieldDist) StdDev() float64 {
	if d.Count < 2 {
		return 0
	}
	v := d.M2 / float64(d.Count-1)
	if v <= 0 {
		return 0
	}
	x := v
	for i := 0; i < 32; i++ {
		x = (x + v/x) / 2
	}
	return x
}

// Snapshot 是监控状态在某一时刻的一致快照。
type Snapshot struct {
	ContractName      string               `json:"contract_name"`
	WatermarkMs       int64                `json:"watermark_ms"`
	LastSeenMs        int64                `json:"last_seen_ms"`
	TotalAccepted     int64                `json:"total_accepted_records"`
	TotalRejected     int64                `json:"total_rejected_records"`
	AcceptedBatches   int64                `json:"accepted_batches"`
	RejectedBatches   int64                `json:"rejected_batches"`
	DuplicateBatches  int64                `json:"duplicate_batches"`
	SidewaysBatches   int64                `json:"sideways_batches"`
	CompletenessRatio float64              `json:"completeness_ratio"`
	FreshnessLagMs    int64                `json:"freshness_lag_ms"`
	Distributions     map[string]FieldDist `json:"distributions"`
	ActiveVersion     string               `json:"active_version"`
	SnapshotTimeMs    int64                `json:"snapshot_time_ms"`
}

// Clone 返回快照的深拷贝，保证调用方修改不会影响引擎内部状态。
func (s *Snapshot) Clone() *Snapshot {
	if s == nil {
		return nil
	}
	cp := *s
	if s.Distributions != nil {
		cp.Distributions = make(map[string]FieldDist, len(s.Distributions))
		for k, v := range s.Distributions {
			cp.Distributions[k] = v
		}
	}
	return &cp
}
