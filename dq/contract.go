package dq

// CompatMode 定义结构兼容性变化时的处理策略。
type CompatMode int

const (
	// CompatReject 结构不兼容时拒绝整个批次。
	CompatReject CompatMode = iota
	// CompatBypass 结构不兼容时旁路：批次不参与校验与指标，仅记录。
	CompatBypass
	// CompatWarn 结构不兼容时仍按旧契约校验，但产生告警。
	CompatWarn
)

// Constraint 是字段级约束。
type Constraint struct {
	Min    *float64 // 数值下界(含)
	Max    *float64 // 数值上界(含)
	MaxLen *int     // 字符串最大长度
}

// FieldSpec 描述契约中的一个字段。
type FieldSpec struct {
	Name       string
	Type       FieldType
	Required   bool
	Constraint *Constraint
}

// Contract 是可版本化的数据契约。
type Contract struct {
	Version  int
	Fields   []FieldSpec
	KeyGroup []string // 组成唯一键的字段组
}

// fieldIndex 返回字段名到规格的索引。
func (c *Contract) fieldIndex() map[string]FieldSpec {
	m := make(map[string]FieldSpec, len(c.Fields))
	for _, f := range c.Fields {
		m[f.Name] = f
	}
	return m
}
