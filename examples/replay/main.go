// Package main 演示 track 引擎的「确定性重放」。
//
// 核心保证：进程崩溃后，引擎依据日志重放可精确复现首次执行——
// 副作用不重复、输出完全一致、日志逐条相同。
//
// 本演示在单进程内复现「首次执行 → 崩溃 → 重放」全过程：
//   - 第 1 阶段：首次执行，录制 journal、产生副作用、记录输出。
//   - 第 2 阶段：模拟崩溃恢复（终态元数据未落盘、实例仍标记为 Running），
//     通过 RunOnce 触发重放。
//
// 运行：go run ./examples/replay
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/engine"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// 两个包级计数器，用于让「重放一致性」肉眼可见：
var (
	// serviceCalls 统计 Execute 副作用执行次数。重放时应为 0（命中日志、跳过执行）。
	serviceCalls int32
	// returnCounter 模拟 Return 参数里的「非确定性源」：每次调用自增。
	// 用它证明：即便重放时业务代码重跑、计数器继续涨，输出仍以日志记录值为准。
	returnCounter int32
)

func main() {
	ctx := context.Background()

	// 装配引擎：内存后端 + 同步执行模式。RunSync 让 Start/RunOnce 同步完成，
	// 便于在单进程内精确控制每一步；NoAutoRecover 避免自动扫描干扰演示。
	// 引擎内部日志经 observ.Logger 输出：桥接 slog 到 stderr 并开启 Debug 级，
	// 让「挂起恢复即重放」的 run replaying 钩子在本演示中可见。
	logger := observ.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelDebug})))
	s := memory.New()
	e := engine.NewEngine(s, engine.WithClock(clock.RealClock{}), engine.WithLogger(logger))
	e.SetTestOptions(engine.TestOptions{RunSync: true, NoAutoRecover: true})
	defer e.Stop(context.Background())

	if err := e.Register("ReplayWorkflow", ReplayWorkflow); err != nil {
		panic(err)
	}

	fmt.Println("track 重放一致性演示")
	fmt.Println("====================")
	fmt.Println("工作流 ReplayWorkflow：3 个幂等 Execute（循环）+ Return（含「非确定性」返回值）。")
	fmt.Println()

	// ---- 第 1 阶段：首次执行（录制 journal）----
	// 阶段标记经同一 logger 输出（与引擎日志同流、时序交错）：
	// 紧跟其后的 run started / run completed 即本阶段触发的那一次 run。
	fmt.Println("=== 第 1 阶段：首次执行（录制 journal）===")
	rid, err := e.Start(ctx, "ReplayWorkflow", nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("  run_id：%s\n", rid)
	first := mustResult(ctx, e, rid)
	golden := mustJournal(ctx, s, rid)

	fmt.Printf("  Execute 副作用调用次数：%d\n", atomic.LoadInt32(&serviceCalls))
	fmt.Printf("  Return 计数器（非确定性源）：%d\n", atomic.LoadInt32(&returnCounter))
	fmt.Printf("  输出：%s\n", string(first.Output))
	printJournal(golden)

	// ---- 第 2 阶段：模拟崩溃恢复（重放）----
	// 真实场景：进程在「业务已执行、终态元数据尚未落盘」之间崩溃，重启后
	// Recover 扫描到仍标记为 Running 的实例并重新调度。此处把状态重置回
	// Running、再调用 RunOnce，等价模拟这一过程。
	fmt.Println()
	fmt.Println("=== 第 2 阶段：模拟崩溃恢复（重放）===")
	fmt.Println("  重置 Execute 副作用计数为 0；Return 计数器【故意保留】，以证明即便")
	fmt.Println("  Return 的业务代码重新执行、计数器自增，输出仍以日志记录值为准。")

	atomic.StoreInt32(&serviceCalls, 0)
	if err := s.UpdateStatus(ctx, rid, model.StatusRunning); err != nil {
		panic(err)
	}
	e.RunOnce(ctx, rid) // 等价于 Recover 触发的重放

	replayed := mustResult(ctx, e, rid)
	replay := mustJournal(ctx, s, rid)
	fmt.Printf("  Execute 副作用调用次数（重放期间）：%d   ✓ 步骤被日志命中，跳过执行\n",
		atomic.LoadInt32(&serviceCalls))
	fmt.Printf("  Return 计数器：%d（业务代码确实重跑了，但输出仍为首次记录值↓）\n",
		atomic.LoadInt32(&returnCounter))
	fmt.Printf("  输出：%s\n", string(replayed.Output))

	// ---- 一致性校验 ----
	outOK := string(first.Output) == string(replayed.Output)
	jrnOK := journalsEqual(golden, replay)

	fmt.Println()
	fmt.Println("=== 一致性校验 ===")
	fmt.Printf("  输出与首次一致：%v\n", outOK)
	fmt.Printf("  日志与首次逐条相同：%v\n", jrnOK)

	fmt.Println()
	fmt.Println("=== 结论 ===")
	fmt.Println("  · Execute：重放命中 KindExec，副作用 0 次（幂等）。")
	fmt.Println("  · Return ：重放命中 KindReturn，记录值权威，忽略重算的非确定性参数。")
	if outOK && jrnOK {
		fmt.Println("  · 崩溃前后输出与日志逐字节一致——确定性重放成立。")
	} else {
		fmt.Println("  · ✗ 一致性校验未通过。")
	}
}

// ReplayWorkflow 演示用工作流：3 个幂等步骤 + 含非确定性源的 Return。
func ReplayWorkflow(wf *engine.WorkflowContext) error {
	var squares []int
	for i := 0; i < 3; i++ {
		v, err := engine.Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&serviceCalls, 1) // 副作用：重放时应被跳过
			return i * i, nil
		}, engine.WithLabel(fmt.Sprintf("square-%d", i)))
		if err != nil {
			return err
		}
		squares = append(squares, v)
	}
	// Return 参数含「非确定性源」：每次调用 returnCounter 自增。重放时业务
	// 代码会重跑到此（计数器继续涨），但 consume(KindReturn) 命中后以历史
	// payload 为准，本次重算的值被忽略——这正是 KindReturn 存在的意义。
	seq := atomic.AddInt32(&returnCounter, 1)
	return wf.Return(map[string]any{"squares": squares, "returnSeq": seq})
}

// mustResult 读取实例元数据。
func mustResult(ctx context.Context, e *engine.Engine, rid model.RunID) *model.RunMeta {
	m, err := e.GetResult(ctx, rid)
	if err != nil {
		panic(err)
	}
	return m
}

// mustJournal 读取实例的全部日志（按追加顺序）。store.Reader 是日志读取的最小接口，
// 任何满足它的后端（如 memory.Store）均可传入。
func mustJournal(ctx context.Context, r store.Reader, rid model.RunID) []model.LogEntry {
	logs, err := r.Read(ctx, rid)
	if err != nil {
		panic(err)
	}
	return logs
}

// printJournal 打印日志条目。
func printJournal(logs []model.LogEntry) {
	fmt.Printf("  journal（%d 条）：\n", len(logs))
	for i, l := range logs {
		label := l.Label
		if label == "" {
			label = "-"
		}
		fmt.Printf("      %02d. kind=%s label=%s payload=%s\n", i+1, l.Kind, label, string(l.Payload))
	}
}

// journalsEqual 比较 Kind/Label/Payload 是否逐条一致（镜像测试中的 logsEqual）。
func journalsEqual(a, b []model.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].Label != b[i].Label ||
			string(a[i].Payload) != string(b[i].Payload) {
			return false
		}
	}
	return true
}
