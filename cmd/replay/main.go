// replay 是本地多批次回放的演示入口：不依赖数据库或消息队列，
// 将预置数据按正序、乱序、重复、迟到等方式回放，打印逐批结论与最终指标，
// 并演示回放过程中交错推进契约版本的效果。
//
// 运行：go run ./cmd/replay
package main

import (
	"fmt"
	"time"

	dq "github.com/h3yow/go-data-quality-monitor-go/dq"
)

func f64(v float64) *float64 { return &v }
func i(v int) *int           { return &v }

// contractV1 初始契约。
func contractV1() dq.Contract {
	return dq.Contract{
		Version: 1,
		Fields: []dq.FieldSpec{
			{Name: "id", Type: dq.TypeInt, Required: true},
			{Name: "name", Type: dq.TypeString, Required: true, Constraint: &dq.Constraint{MaxLen: i(16)}},
			{Name: "amount", Type: dq.TypeFloat, Required: true, Constraint: &dq.Constraint{Min: f64(0), Max: f64(10000)}},
			{Name: "note", Type: dq.TypeString, Required: false},
		},
		KeyGroup: []string{"id"},
	}
}

// contractV2 兼容演进：amount 上限放宽、note 仍为可选、新增可选字段 tag。
func contractV2() dq.Contract {
	c := contractV1()
	c.Version = 2
	c.Fields[2].Constraint = &dq.Constraint{Min: f64(0), Max: f64(50000)}
	c.Fields = append(c.Fields, dq.FieldSpec{Name: "tag", Type: dq.TypeString, Required: false})
	return c
}

// contractV3Bad 不兼容演进：name 长度收紧、note 由可选变必需。
func contractV3Bad() dq.Contract {
	c := contractV2()
	c.Version = 3
	c.Fields[1].Constraint = &dq.Constraint{MaxLen: i(8)}
	c.Fields[3].Required = true
	return c
}

// makeBatches 预置 6 批数据，含合法记录与若干违规记录。
func makeBatches(base time.Time) []dq.Batch {
	rec := func(id int, name string, amount float64) dq.Record {
		return dq.Record{"id": id, "name": name, "amount": amount}
	}
	return []dq.Batch{
		{ID: "b1", Seq: 1, EventTime: base, Records: []dq.Record{rec(1, "alice", 10), rec(2, "bob", 20)}},
		{ID: "b2", Seq: 2, EventTime: base.Add(time.Minute), Records: []dq.Record{rec(3, "carol", 30), rec(3, "carol-dup", 31)}}, // 批内键重复
		{ID: "b3", Seq: 3, EventTime: base.Add(2 * time.Minute), Records: []dq.Record{rec(4, "dave", -5)}},                        // 约束越界
		{ID: "b4", Seq: 4, EventTime: base.Add(3 * time.Minute), Records: []dq.Record{rec(5, "erin", 50), {"id": 6, "amount": 60}}}, // 缺字段
		{ID: "b5", Seq: 5, EventTime: base.Add(4 * time.Minute), Records: []dq.Record{rec(7, "frank", 70)}},
		{ID: "b6", Seq: 6, EventTime: base.Add(5 * time.Minute), Records: []dq.Record{rec(8, "grace", 80), rec(9, "heidi", 90)}},
	}
}

func printReport(title string, rep dq.ReplayReport) {
	fmt.Printf("== %s ==\n", title)
	for _, line := range rep.Timeline {
		fmt.Printf("  %s\n", line)
	}
	for _, o := range rep.Outcomes {
		for _, v := range o.Violations {
			fmt.Printf("      violation batch=%s row=%d reason=%s field=%s %s\n",
				o.BatchID, v.Row, v.Reason, v.Field, v.Detail)
		}
	}
	for _, ev := range rep.Evolutions {
		fmt.Printf("  %s\n", ev.Report.Summary())
	}
	m := rep.Metrics
	fmt.Printf("  metrics: watermark=%d seen=%d accepted=%d violations=%d alerts=%d\n",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations, len(m.DriftAlerts))
	for _, w := range rep.Warnings {
		fmt.Printf("  warning: %s\n", w)
	}
	fmt.Println()
}

func main() {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base.Add(10 * time.Minute) } // 固定时钟，输出可复现

	// 场景 1：正序回放。
	r1 := dq.NewReplayer(contractV1(), dq.WithClock(clock))
	rep1 := r1.RunBatches(makeBatches(base))
	printReport("场景1 正序回放", rep1)

	// 场景 2：乱序 + 重复投递 + 迟到批次，最终指标应与场景 1 一致。
	batches := makeBatches(base)
	scrambled := dq.DeferLast(dq.Permute(batches, 42), "b2") // 打乱后让 b2 迟到
	scrambled = dq.Redeliver(scrambled, "b1", "b3")          // b1/b3 重复投递
	r2 := dq.NewReplayer(contractV1(), dq.WithClock(clock))
	rep2 := r2.RunBatches(scrambled)
	printReport("场景2 乱序+重复+迟到回放", rep2)

	m1, m2 := rep1.Metrics, rep2.Metrics
	fmt.Printf("场景1/场景2 关键指标一致: %v (watermark %d vs %d, accepted %d vs %d, violations %d vs %d)\n\n",
		m1.Watermark == m2.Watermark && m1.TotalAccepted == m2.TotalAccepted && m1.TotalViolations == m2.TotalViolations,
		m1.Watermark, m2.Watermark, m1.TotalAccepted, m2.TotalAccepted, m1.TotalViolations, m2.TotalViolations)

	// 场景 3：回放中交错契约演进——兼容演进生效，不兼容演进被拒绝，后续批次按当时版本判定。
	r3 := dq.NewReplayer(contractV1(), dq.WithClock(clock), dq.WithCompatMode(dq.CompatReject))
	script := []dq.ReplayEvent{
		dq.SubmitEvent(batches[0]),
		dq.EvolveEvent(contractV2()),   // 兼容：放宽上限 + 新增可选字段
		dq.SubmitEvent(batches[1]),     // 按 v2 判定
		dq.EvolveEvent(contractV3Bad()), // 不兼容：长度收紧 + 可选变必需，被拒绝
		dq.SubmitEvent(batches[2]),     // 仍按 v2 判定
		dq.SubmitEvent(batches[3]),
	}
	rep3 := r3.Run(script)
	printReport("场景3 回放中交错契约演进(CompatReject)", rep3)
}
