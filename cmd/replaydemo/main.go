// 命令 replaydemo 在本地(无数据库/消息队列)重放一段多批次数据流，
// 演示乱序、重复、迟到批次，以及契约演进与批次提交交错时的判定。
//
// 运行：go run ./cmd/replaydemo
package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/h3yow/go-data-quality-monitor-go/dq"
)

func v1() dq.Contract {
	maxLen := 10
	return dq.Contract{
		Version: 1,
		Fields: []dq.FieldSpec{
			{Name: "id", Type: dq.TypeInt, Required: true},
			{Name: "amount", Type: dq.TypeFloat, Required: true},
			{Name: "name", Type: dq.TypeString, Required: false, Constraint: &dq.Constraint{MaxLen: &maxLen}},
		},
		KeyGroup: []string{"id"},
	}
}

// v2 兼容放宽：name 长度上限 10 -> 20，新增可选字段 note。
func v2() dq.Contract {
	maxLen := 20
	c := v1()
	c.Version = 2
	c.Fields[2].Constraint = &dq.Constraint{MaxLen: &maxLen}
	c.Fields = append(c.Fields, dq.FieldSpec{Name: "note", Type: dq.TypeString, Required: false})
	return c
}

// v3 收紧：name 从可选变必填，并给 amount 增加下界 0。
func v3() dq.Contract {
	c := v2()
	c.Version = 3
	c.Fields[2].Required = true
	c.Fields[1].Constraint = &dq.Constraint{Min: ptr(0.0)}
	return c
}

// v4 兼容放宽：amount 从必填变为可选。
func v4() dq.Contract {
	c := v2()
	c.Version = 4
	c.Fields[1].Required = false
	return c
}

func ptr[T any](v T) *T { return &v }

// script 构造回放脚本：乱序、重复、迟到，演进与提交交错。
func script(base time.Time) []dq.Step {
	batch := func(id string, seq uint64, recs ...dq.Record) dq.Batch {
		return dq.Batch{ID: id, Seq: uint64(seq), Records: recs,
			EventTime: base.Add(time.Duration(seq) * time.Minute)}
	}
	return []dq.Step{
		{Kind: dq.StepSubmit, Label: "正常批次", Batch: batch("b1", 1,
			dq.Record{"id": 1, "amount": 10.0, "name": "alice"},
			dq.Record{"id": 2, "amount": 20.0, "name": "bob"},
		)},
		{Kind: dq.StepSubmit, Label: "乱序:先到 seq=3(seq=2 缺口)", Batch: batch("b3", 3,
			dq.Record{"id": 3, "amount": 30.0, "name": "carol"},
			dq.Record{"id": 3, "amount": 30.0, "name": "carol-dup"}, // 批内键重复
		)},
		{Kind: dq.StepSubmit, Label: "补齐 seq=2,水位应追到 3", Batch: batch("b2", 2,
			dq.Record{"id": 4, "name": "dave"}, // 缺 amount -> field_missing
		)},
		{Kind: dq.StepSubmit, Label: "重复投递 b1", Batch: batch("b1", 1,
			dq.Record{"id": 1, "amount": 10.0, "name": "alice"},
			dq.Record{"id": 2, "amount": 20.0, "name": "bob"},
		)},
		{Kind: dq.StepSubmit, Label: "先到 seq=5(seq=4 缺口)", Batch: batch("b5", 5,
			dq.Record{"id": 5, "amount": 50.0, "name": strings.Repeat("e", 9)}, // v1 下 9 字符合法
		)},
		{Kind: dq.StepEvolve, Label: "兼容放宽到 v2: name 长度 10->20 + 新增可选 note", Contract: v2()},
		{Kind: dq.StepSubmit, Label: "迟到批次 seq=4: 15 字符 name 在 v1 会违规, v2 下合法",
			Batch: batch("b4", 4,
				dq.Record{"id": 6, "amount": 40.0, "name": strings.Repeat("f", 15)},
			)},
		{Kind: dq.StepEvolve, Label: "收紧到 v3: name 变必填 + amount 引入下界(应被拒绝)", Contract: v3()},
		{Kind: dq.StepSubmit, Label: "拒绝后仍按 v2 判定: name 可缺失", Batch: batch("b6", 6,
			dq.Record{"id": 7, "amount": 60.0},
		)},
		{Kind: dq.StepEvolve, Label: "兼容放宽到 v4: amount 必填 -> 可选", Contract: v4()},
		{Kind: dq.StepSubmit, Label: "v4 下 amount 可缺失, 应全部接受", Batch: batch("b7", 7,
			dq.Record{"id": 8, "name": "heidi"},
		)},
	}
}

func printStep(s dq.StepResult) {
	switch s.Kind {
	case dq.StepSubmit:
		v := s.Validation
		fmt.Printf("[%02d] submit   batch=%-3s seq=%d contract=v%d label=%s\n",
			s.Index, s.BatchID, s.Seq, v.ContractVersion, s.Label)
		if s.Err != nil {
			fmt.Printf("       ERROR %v\n", s.Err)
		}
		fmt.Printf("       accepted=%d violations=%d\n", len(v.Accepted), len(v.Violations))
		for _, viol := range v.Violations {
			fmt.Printf("       - row=%d reason=%s field=%s detail=%s\n",
				viol.Row, viol.Reason, viol.Field, viol.Detail)
		}
	case dq.StepEvolve:
		r := s.Evolution
		fmt.Printf("[%02d] evolve   v%d -> v%d action=%s label=%s\n",
			s.Index, r.FromVersion, r.ToVersion, r.Action, s.Label)
		for _, c := range r.Changes {
			mark := "ok "
			if !c.Compatible {
				mark = "BAD"
			}
			fmt.Printf("       %s %s\n", mark, c)
		}
		if r.Err != nil {
			fmt.Printf("       ERROR %v\n", r.Err)
		}
	}
}

func printMetrics(m dq.Metrics) {
	fmt.Printf("watermark=%d seenBatches=%d accepted=%d violations=%d lag=%s\n",
		m.Watermark, m.SeenBatches, m.TotalAccepted, m.TotalViolations, m.FreshnessLag)
	fmt.Printf("completeness=%v\n", m.Completeness)
	fmt.Printf("driftBaseline=%v alerts=%v\n", m.DriftBaseline, m.DriftAlerts)
}

// runReplay 用 Replayer 跑脚本。
func runReplay(ctx context.Context, base time.Time) dq.ReplayReport {
	fixedNow := func() time.Time { return base.Add(10 * time.Minute) }
	r := dq.NewReplayer(v1(), dq.WithCompatMode(dq.CompatReject), dq.WithClock(fixedNow))
	return r.Run(ctx, script(base))
}

// runManual 不使用 Replayer，手动按同一顺序逐批 Submit / EvolveDetailed，
// 用于证明回放结论与手工提交完全一致。
func runManual(ctx context.Context, base time.Time) dq.ReplayReport {
	fixedNow := func() time.Time { return base.Add(10 * time.Minute) }
	e := dq.NewEngine(v1(), dq.WithCompatMode(dq.CompatReject), dq.WithClock(fixedNow))
	var steps []dq.StepResult
	for i, s := range script(base) {
		switch s.Kind {
		case dq.StepEvolve:
			rep := e.EvolveDetailed(s.Contract)
			steps = append(steps, dq.StepResult{Index: i, Label: s.Label, Kind: s.Kind, Evolution: &rep, Err: rep.Err})
		default:
			res, err := e.Submit(ctx, s.Batch)
			steps = append(steps, dq.StepResult{Index: i, Label: s.Label, Kind: s.Kind,
				BatchID: s.Batch.ID, Seq: s.Batch.Seq, Validation: &res, Err: err})
		}
	}
	return dq.ReplayReport{Steps: steps, Metrics: e.Metrics(), Warnings: e.Warnings(),
		ContractVersion: e.Contract().Version}
}

func main() {
	ctx := context.Background()
	base := time.Date(2026, 10, 6, 9, 0, 0, 0, time.UTC)

	rep := runReplay(ctx, base)
	fmt.Println("=== 本地多批次回放(无外部依赖) ===")
	for _, s := range rep.Steps {
		printStep(s)
	}
	fmt.Println("\n=== 最终指标 ===")
	printMetrics(rep.Metrics)
	fmt.Printf("contract=v%d warnings=%v\n", rep.ContractVersion, rep.Warnings)

	// 同一脚本重复跑：结论必须一致。
	rep2 := runReplay(ctx, base)
	fmt.Println("\n=== 可复现性校验 ===")
	fmt.Printf("同一脚本重放两次完全一致: %v\n", reflect.DeepEqual(rep, rep2))

	// 回放 vs 手动逐批提交：结论必须一致。
	manual := runManual(ctx, base)
	fmt.Printf("回放与手动逐批提交完全一致: %v\n",
		reflect.DeepEqual(normalize(rep), normalize(manual)))

	fmt.Printf("\n观察点: 乱序/迟到批次只在缺口补齐后推进水位; 重复 b1 只计一次;\n" +
		"收紧演进被拒绝后 b6 仍按 v2 判定(name 可缺); 放宽生效后 b7 按 v4 接受缺 amount 的记录。\n")
}

// normalize 比较时只保留结论性字段(两种执行路径的指针地址不同)。
func normalize(r dq.ReplayReport) any {
	type step struct {
		Index, Label, Kind, BatchID string
		Seq                         uint64
		Version                     int
		Accepted, Violations        int
		Reasons                     []string
		Action                      dq.EvolutionAction
		Changes                     []dq.Change
		HasErr                      bool
	}
	var out []step
	for _, s := range r.Steps {
		switch s.Kind {
		case dq.StepSubmit:
			st := step{Index: fmt.Sprint(s.Index), Label: s.Label, Kind: string(s.Kind),
				BatchID: s.BatchID, Seq: s.Seq, Version: s.Validation.ContractVersion,
				Accepted: len(s.Validation.Accepted), Violations: len(s.Validation.Violations),
				HasErr: s.Err != nil}
			for _, v := range s.Validation.Violations {
				st.Reasons = append(st.Reasons, string(v.Reason)+":"+v.Field)
			}
			out = append(out, st)
		case dq.StepEvolve:
			out = append(out, step{Index: fmt.Sprint(s.Index), Label: s.Label, Kind: string(s.Kind),
				Action: s.Evolution.Action, Changes: s.Evolution.Changes, HasErr: s.Err != nil})
		}
	}
	return struct {
		Steps    []step
		Metrics  dq.Metrics
		Version  int
		Warnings []string
	}{out, r.Metrics, r.ContractVersion, r.Warnings}
}
