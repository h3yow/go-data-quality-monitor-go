// Package monitor 实现跨批次增量监控：水位、完整性、新鲜度与分布漂移基线。
// 重复批次幂等（由引擎层 dedup 保证），乱序批次不使水位/基线倒退。
package monitor

import (
	"sync"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// Options 控制监控行为。
type Options struct {
	// AllowedLatenessMs 允许的乱序迟到窗口。
	// eventTime + AllowedLatenessMs < watermark 的批次视为迟到：
	// 仍参与记录计数与完整性统计，但不推进水位、不更新分布基线并告警。
	AllowedLatenessMs int64
	// WindowMs 完整性滚动窗口（以水位为右界）；0 表示全量统计。
	WindowMs int64
	// FreezeAfter 分布基线收集满该条合法记录后冻结，之后只做漂移检测。
	FreezeAfter int64
	// DriftZ 触发分布漂移告警的 z-score 阈值（批均值相对基线）。
	DriftZ float64
	// FreshnessAlertMs 水位滞后墙钟超过该值时产生 freshness 告警；0 关闭。
	FreshnessAlertMs int64
}

// DefaultOptions 返回默认监控参数。
func DefaultOptions() Options {
	return Options{
		AllowedLatenessMs: 60_000,
		WindowMs:          0,
		FreezeAfter:       200,
		DriftZ:            3.0,
		FreshnessAlertMs:  300_000,
	}
}

// ApplyRequest 是提交一个已校验批次给监控器的请求。
type ApplyRequest struct {
	Batch         model.Batch
	ContractName  string
	ValidCount    int
	InvalidCount  int
	ReasonCounts  map[model.Reason]int
	RecordResults []model.RecordResult
	// Sideways 为 true 时该批次按旁路处理：单独计数，不进主基线/水位。
	Sideways bool
	// SeenTimeMs 引擎接收时刻（毫秒 epoch）。
	SeenTimeMs int64
}

// ApplyResult 是监控器的提交结论。
type ApplyResult struct {
	Outcome  model.Outcome
	Alerts   []model.Alert
	Snapshot *model.Snapshot
}

// Monitor 是单个数据集（契约名）的增量监控器。
type Monitor interface {
	// Apply 幂等应用一个批次（调用方须保证 BatchID 不重复）。
	Apply(req ApplyRequest) ApplyResult
	// SetActiveVersion 更新快照中报告的生效契约版本。
	SetActiveVersion(v string)
	// Snapshot 返回监控状态的一致深拷贝快照。
	Snapshot() *model.Snapshot
}

type windowPoint struct {
	eventTimeMs int64
	valid       int64
	invalid     int64
}

type stateMonitor struct {
	mu           sync.Mutex
	name         string
	opts         Options
	now          func() int64
	watermark    int64
	lastSeen     int64
	totalValid   int64
	totalInvalid int64
	accepted     int64
	rejected     int64
	sideways     int64
	dist         map[string]*model.FieldDist
	points       []windowPoint // 按 eventTime 单调插入，用于窗口完整性
	activeVer    string
}

// New 创建监控器，nowFn 用于注入时钟（测试），nil 表示墙钟。
func New(contractName string, opts Options, nowFn func() int64) Monitor {
	if nowFn == nil {
		nowFn = nowMillis
	}
	return &stateMonitor{
		name: contractName,
		opts: opts,
		now:  nowFn,
		dist: map[string]*model.FieldDist{},
	}
}

// SetActiveVersion 更新当前生效契约版本（由引擎在锁内调用场景之外使用，内部自带锁）。
func (m *stateMonitor) SetActiveVersion(v string) {
	m.mu.Lock()
	m.activeVer = v
	m.mu.Unlock()
}

// Apply 在互斥临界区内应用批次，保证快照语义：批次要么整体可见，要么不可见。
func (m *stateMonitor) Apply(req ApplyRequest) ApplyResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastSeen = req.SeenTimeMs
	if req.ContractName != "" {
		m.name = req.ContractName
	}

	if req.Sideways {
		m.sideways++
		return ApplyResult{
			Outcome:  model.OutcomeSideways,
			Snapshot: m.snapshotLocked(),
		}
	}

	// 所有非旁路批次都进入记录计数与完整性窗口。
	m.totalValid += int64(req.ValidCount)
	m.totalInvalid += int64(req.InvalidCount)

	late := req.Batch.EventTimeMillis+m.opts.AllowedLatenessMs < m.watermark
	m.insertPoint(req.Batch.EventTimeMillis, int64(req.ValidCount), int64(req.InvalidCount))

	var alerts []model.Alert
	if late {
		alerts = append(alerts, model.Alert{
			BatchID: req.Batch.BatchID, ContractV: req.Batch.ContractV,
			Kind: "late_batch", EventTimeMs: req.Batch.EventTimeMillis, SeenTimeMs: req.SeenTimeMs,
			Detail: "event time is beyond allowed lateness; counted but excluded from watermark and baseline",
		})
	}

	// 水位与分布基线只被"完全合法且非迟到"的批次推进：
	// - 含非法记录/被拒批次不能用其事件时间污染水位；
	// - 乱序批次不使水位倒退（仅接受更大的事件时间）。
	clean := req.InvalidCount == 0 && !late
	if clean && req.Batch.EventTimeMillis > m.watermark {
		m.watermark = req.Batch.EventTimeMillis
	}

	// 分布基线只被干净批次的合法记录更新；基线冻结后只检测漂移。
	alerts = append(alerts, m.updateDistributionsLocked(req, clean)...)

	if lag := m.now() - m.watermark; m.opts.FreshnessAlertMs > 0 && lag > m.opts.FreshnessAlertMs {
		alerts = append(alerts, model.Alert{
			BatchID: req.Batch.BatchID, ContractV: req.Batch.ContractV,
			Kind: "freshness", EventTimeMs: req.Batch.EventTimeMillis, SeenTimeMs: req.SeenTimeMs,
			Detail: "watermark lags wall clock beyond freshness threshold",
		})
	}

	// 批次级结论：存在非法记录即 rejected，但指标已按 valid/invalid 分类计数，
	// 水位不回退（水位是数据时间的事实，与质量结论正交）。
	if req.InvalidCount > 0 {
		m.rejected++
	} else {
		m.accepted++
	}
	outcome := model.OutcomeAccepted
	if req.InvalidCount > 0 {
		outcome = model.OutcomeRejected
	}
	return ApplyResult{Outcome: outcome, Alerts: alerts, Snapshot: m.snapshotLocked()}
}

func (m *stateMonitor) Snapshot() *model.Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

// snapshotLocked 构造深拷贝快照，调用方须持有 mu。
func (m *stateMonitor) snapshotLocked() *model.Snapshot {
	dists := make(map[string]model.FieldDist, len(m.dist))
	for k, v := range m.dist {
		dists[k] = *v
	}
	valid, invalid := m.windowCountsLocked()
	total := valid + invalid
	ratio := 1.0
	if total > 0 {
		ratio = float64(valid) / float64(total)
	}
	return &model.Snapshot{
		ContractName:      m.name,
		WatermarkMs:       m.watermark,
		LastSeenMs:        m.lastSeen,
		TotalAccepted:     m.totalValid,
		TotalRejected:     m.totalInvalid,
		AcceptedBatches:   m.accepted,
		RejectedBatches:   m.rejected,
		SidewaysBatches:   m.sideways,
		CompletenessRatio: ratio,
		FreshnessLagMs:    m.now() - m.watermark,
		Distributions:     dists,
		ActiveVersion:     m.activeVer,
		SnapshotTimeMs:    m.now(),
	}
}

// insertPoint 按事件时间有序插入窗口点，乱序批次不会破坏单调结构。
func (m *stateMonitor) insertPoint(et int64, valid, invalid int64) {
	p := windowPoint{eventTimeMs: et, valid: valid, invalid: invalid}
	idx := len(m.points)
	for i := len(m.points) - 1; i >= 0; i-- {
		if m.points[i].eventTimeMs <= et {
			idx = i + 1
			break
		}
		idx = i
	}
	m.points = append(m.points, windowPoint{})
	copy(m.points[idx+1:], m.points[idx:])
	m.points[idx] = p
}

// windowCountsLocked 统计完整性窗口内的合法/非法记录数。
func (m *stateMonitor) windowCountsLocked() (int64, int64) {
	if m.opts.WindowMs <= 0 {
		return m.totalValid, m.totalInvalid
	}
	lo := m.watermark - m.opts.WindowMs
	var v, iv int64
	for _, p := range m.points {
		if p.eventTimeMs >= lo && p.eventTimeMs <= m.watermark {
			v += p.valid
			iv += p.invalid
		}
	}
	return v, iv
}
