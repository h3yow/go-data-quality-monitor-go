package dq

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Metrics 是增量监控的关键指标快照。
type Metrics struct {
	Watermark       uint64             // 已连续确认的最大批次序号(从 1 起)
	SeenBatches     int                // 已去重处理的批次数
	TotalAccepted   int                // 累计接受记录数(去重后)
	TotalViolations int                // 累计违规记录数(去重后)
	Completeness    map[string]float64 // 各契约字段在全部记录中的非空率 [0,1]
	FreshnessLag    time.Duration      // 当前时间与最新事件时间的滞后
	DriftBaseline   map[string]float64 // 数值字段分布基线(运行均值)
	DriftAlerts     []string           // 漂移告警(按发生顺序)
}

// driftMinSamples 是基线参与漂移判定所需的最小样本数。
const driftMinSamples = 10

// driftSigma 是漂移告警的 sigma 倍数阈值。
const driftSigma = 3.0

// fieldStat 是单数值字段的 Welford 运行统计。
type fieldStat struct {
	count int
	mean  float64
	m2    float64
}

func (s *fieldStat) add(x float64) {
	s.count++
	d := x - s.mean
	s.mean += d / float64(s.count)
	s.m2 += d * (x - s.mean)
}

func (s *fieldStat) stddev() float64 {
	if s.count < 2 {
		return 0
	}
	return math.Sqrt(s.m2 / float64(s.count-1))
}

// Monitor 维护跨批次增量监控状态。
// 所有方法并发安全；重复批次(相同 Batch.ID)只会计入一次，
// 乱序批次不会导致水位倒退或重复计数。
type Monitor struct {
	mu            sync.Mutex
	seen          map[string]struct{}  // 已处理批次 ID
	seqs          map[uint64]struct{}  // 已观察批次序号
	watermark     uint64               // 连续确认水位
	totalAccepted int
	totalViol     int
	fieldPresent  map[string]int // 字段非空记录数
	recordTotal   int            // 已观察记录总数
	maxEventTime  time.Time      // 最新事件时间(单调不减)
	stats         map[string]*fieldStat
	alerts        []string
	now           func() time.Time // 可注入时钟，便于测试
}

// NewMonitor 创建监控器。
func NewMonitor() *Monitor {
	return &Monitor{
		seen:         make(map[string]struct{}),
		seqs:         make(map[uint64]struct{}),
		fieldPresent: make(map[string]int),
		stats:        make(map[string]*fieldStat),
		now:          time.Now,
	}
}

// Observe 消费一个批次的校验结果并更新指标。
// 相同 Batch.ID 的重复提交是幂等的(直接忽略)；
// 乱序批次会被记录但只在序号连续时推进水位。
// c 为校验该批次所用的契约，用于完整性统计。
func (m *Monitor) Observe(b Batch, res ValidationResult, c Contract) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, dup := m.seen[b.ID]; dup {
		return // 重复批次：幂等忽略，不重复计数
	}
	m.seen[b.ID] = struct{}{}

	// 水位：记录序号，推进连续前缀。
	m.seqs[b.Seq] = struct{}{}
	for {
		if _, ok := m.seqs[m.watermark+1]; ok {
			m.watermark++
		} else {
			break
		}
	}

	m.totalAccepted += len(res.Accepted)
	m.totalViol += len(res.Violations)

	// 完整性：对契约声明字段统计非空率(覆盖全部输入记录)。
	for _, r := range b.Records {
		m.recordTotal++
		for _, f := range c.Fields {
			if v, ok := r[f.Name]; ok && v != nil {
				m.fieldPresent[f.Name]++
			}
		}
	}

	// 新鲜度：事件时间只前进不倒退。
	if b.EventTime.After(m.maxEventTime) {
		m.maxEventTime = b.EventTime
	}

	// 分布漂移：先判定(相对旧基线)，再把本批数据并入基线。
	numeric := numericFields(c)
	batchSum := make(map[string]float64, len(numeric))
	batchCnt := make(map[string]int, len(numeric))
	for _, r := range res.Accepted {
		for _, name := range numeric {
			if v, ok := r[name]; ok {
				if f, ok := toFloat(v); ok {
					batchSum[name] += f
					batchCnt[name]++
				}
			}
		}
	}
	for _, name := range numeric {
		cnt := batchCnt[name]
		if cnt == 0 {
			continue
		}
		batchMean := batchSum[name] / float64(cnt)
		st := m.stats[name]
		if st == nil {
			st = &fieldStat{}
			m.stats[name] = st
		}
		if st.count >= driftMinSamples {
			sd := st.stddev()
			if sd > 0 && math.Abs(batchMean-st.mean) > driftSigma*sd {
				m.alerts = append(m.alerts, fmt.Sprintf(
					"batch %s: field %s mean %.4f deviates from baseline %.4f by %.1f sigma",
					b.ID, name, batchMean, st.mean, math.Abs(batchMean-st.mean)/sd))
			}
		}
		// 并入基线(逐条重放该批均值对应的聚合，保证基线只增不减样本)。
		st.add(batchMean) // 以批均值为样本更新基线
	}
}

// numericFields 返回契约中数值字段名(确定性顺序)。
func numericFields(c Contract) []string {
	var out []string
	for _, f := range c.Fields {
		if f.Type == TypeInt || f.Type == TypeFloat {
			out = append(out, f.Name)
		}
	}
	return out
}

// Seen 报告批次 ID 是否已被监控计入(用于判定重复投递)。
func (m *Monitor) Seen(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.seen[id]
	return ok
}

// Snapshot 返回当前指标的一致性快照(深拷贝，可安全并发读取)。
func (m *Monitor) Snapshot() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := Metrics{
		Watermark:       m.watermark,
		SeenBatches:     len(m.seen),
		TotalAccepted:   m.totalAccepted,
		TotalViolations: m.totalViol,
		Completeness:    make(map[string]float64, len(m.fieldPresent)),
		DriftBaseline:   make(map[string]float64, len(m.stats)),
		DriftAlerts:     append([]string(nil), m.alerts...),
	}
	if m.recordTotal > 0 {
		for name, cnt := range m.fieldPresent {
			out.Completeness[name] = float64(cnt) / float64(m.recordTotal)
		}
	}
	if !m.maxEventTime.IsZero() {
		out.FreshnessLag = m.now().Sub(m.maxEventTime)
		if out.FreshnessLag < 0 {
			out.FreshnessLag = 0
		}
	}
	for name, st := range m.stats {
		out.DriftBaseline[name] = st.mean
	}
	return out
}

// SortedAlerts 返回按字典序排序的告警副本，便于测试断言可复现。
func (m Metrics) SortedAlerts() []string {
	out := append([]string(nil), m.DriftAlerts...)
	sort.Strings(out)
	return out
}
