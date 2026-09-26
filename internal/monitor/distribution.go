package monitor

import (
	"fmt"

	"github.com/h3yow/go-data-quality-monitor-go/internal/model"
)

// updateDistributionsLocked 用批次中的合法记录更新数值字段分布基线。
// update=true 且基线未冻结时执行 Welford 增量统计；
// 基线冻结后仅用批均值做 z-score 漂移检测，不修改基线（基线不可倒退/漂移）。
func (m *stateMonitor) updateDistributionsLocked(req ApplyRequest, update bool) []model.Alert {
	// 先汇总本批次每个数值字段的（合法记录）值列表。
	values := map[string][]float64{}
	for _, rr := range req.RecordResults {
		if !rr.Valid {
			continue
		}
		rec := req.Batch.Records[rr.Index]
		for k, v := range rec {
			if n, ok := numericValue(v); ok {
				values[k] = append(values[k], n)
			}
		}
	}

	var alerts []model.Alert
	for field, vs := range values {
		d, exists := m.dist[field]
		if !exists {
			d = &model.FieldDist{Field: field}
			m.dist[field] = d
		}
		var sum float64
		for _, x := range vs {
			sum += x
		}
		batchMean := sum / float64(len(vs))

		if d.Count >= m.opts.FreezeAfter {
			// 基线已冻结：漂移检测。
			sd := d.StdDev()
			if sd > 0 {
				z := (batchMean - d.Mean) / sd
				if z > m.opts.DriftZ || z < -m.opts.DriftZ {
					alerts = append(alerts, model.Alert{
						BatchID:     req.Batch.BatchID,
						ContractV:   req.Batch.ContractV,
						Kind:        "distribution_drift",
						Detail:      fmt.Sprintf("field %s batch mean %.4f vs baseline mean %.4f (z=%.2f)", field, batchMean, d.Mean, z),
						EventTimeMs: req.Batch.EventTimeMillis,
						SeenTimeMs:  req.SeenTimeMs,
					})
				}
			}
			continue
		}
		if !update {
			// 迟到批次不污染基线。
			continue
		}
		// Welford 在线更新；Min/Max 单调扩展。
		for _, x := range vs {
			d.Count++
			delta := x - d.Mean
			d.Mean += delta / float64(d.Count)
			d.M2 += delta * (x - d.Mean)
			if d.Count == 1 {
				d.Min, d.Max = x, x
			} else {
				if x < d.Min {
					d.Min = x
				}
				if x > d.Max {
					d.Max = x
				}
			}
			if d.Count == m.opts.FreezeAfter {
				d.FrozenAt = req.SeenTimeMs
			}
		}
	}
	return alerts
}

func numericValue(v any) (float64, bool) {
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
	default:
		return 0, false
	}
}
