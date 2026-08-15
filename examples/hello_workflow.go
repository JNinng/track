// Package main 演示如何使用 track 工作流引擎定义并运行一个工作流。
//
// 运行：go run ./examples
//
// 本示例展示一个典型的工作流：读取输入 -> 幂等执行一个步骤 -> 睡眠 ->
// 等待外部信号 -> 返回结果。引擎在内部以日志驱动、确定性重放的方式执行，
// 进程崩溃后可完全一致地恢复。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/engine"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// HelloInput 是 HelloWorkflow 的启动参数。
type HelloInput struct {
	Name string
}

func main() {
	// 1. 装配引擎：内存后端 + 默认 Worker Pool。
	// 引擎内部日志经 observ.Logger 输出：此处桥接 slog 到 stderr，并开启
	// Debug 级以展示完整时间线（唤醒调度/触发、重放等）；
	// 不注入也不设置默认 Logger 时，引擎保持零输出。
	logger := observ.NewSlogLogger(slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelDebug})))
	store := memory.New()
	e := engine.NewEngine(store, engine.WithWorkers(4), engine.WithClock(clock.RealClock{}),
		engine.WithLogger(logger))
	defer e.Stop(context.Background())

	// 2. 注册工作流。
	if err := e.Register("HelloWorkflow", HelloWorkflow); err != nil {
		logger.Log(slog.LevelError, "register workflow failed", slog.Any("error", err))
		os.Exit(1)
	}

	// 3. 启动一个新实例。
	runID, err := e.Start(context.Background(), "HelloWorkflow", HelloInput{Name: "World"})
	if err != nil {
		logger.Log(slog.LevelError, "start workflow failed", slog.Any("error", err))
		os.Exit(1)
	}
	fmt.Println("started run:", runID)

	// 4. 投递信号：工作流先 Sleep（50ms）再 Await "approved" 信号，
	// 由 sendApprovalSignal 模拟外部系统（审批回调）在工作流挂起期间批准请求。
	go sendApprovalSignal(e, logger, runID)

	// 5. 轮询结果（生产环境可用事件 / 回调替代轮询）。
	result, err := wait(context.Background(), e, runID, 5*time.Second)
	if err != nil {
		logger.Log(slog.LevelError, "wait result failed", slog.Any("error", err))
		os.Exit(1)
	}

	fmt.Printf("status: %s\n", result.Status)
	fmt.Printf("output: %s\n", string(result.Output))
	if result.Err != "" {
		fmt.Printf("error:  %s\n", result.Err)
	}

	// 6. 读取该实例的全部日志（journal）并打印。
	logs, err := readJournal(context.Background(), store, runID)
	if err != nil {
		logger.Log(slog.LevelError, "read journal failed", slog.Any("error", err))
		os.Exit(1)
	}
	fmt.Println("journal:")
	for i, l := range logs {
		label := l.Label
		if label == "" {
			label = "-"
		}
		fmt.Printf("  %02d. kind=%s label=%s payload=%s\n", i+1, l.Kind, label, string(l.Payload))
	}
}

// sendApprovalSignal 演示引擎的 Signal API：向挂起的实例投递信号并触发恢复。
//
// 真实场景中信号来自外部系统（审批回调、Webhook、消息队列消费者）——
// Signal 是引擎的入站 API。这里在短暂延迟后投递，模拟「工作流 Await 挂起
// 期间，外部审批系统批准了该请求」。
func sendApprovalSignal(e *engine.Engine, logger observ.Logger, runID model.RunID) {
	time.Sleep(100 * time.Millisecond)
	fmt.Println("sending signal: approved")
	if err := e.Signal(context.Background(), runID, "approved", map[string]string{"by": "ops"}); err != nil {
		logger.Log(slog.LevelWarn, "signal error", slog.Any("error", err))
	}
}

// readJournal 返回指定运行实例的全部日志（按追加顺序）。
// store.Reader 是日志读取的最小接口，任何满足它的后端（如 memory.Store）均可传入。
func readJournal(ctx context.Context, r store.Reader, runID model.RunID) ([]model.LogEntry, error) {
	return r.Read(ctx, runID)
}

// HelloWorkflow 是业务工作流。它通过注入的 *engine.WorkflowContext 调用各原语。
//
// 注意：所有原语（Input / Execute / Sleep / Await / Return）都是幂等且可重放的，
// 因此本函数被重放时不会产生重复副作用。
func HelloWorkflow(wf *engine.WorkflowContext) error {
	// 6.1 Input：读取启动参数。
	in, err := engine.Input[HelloInput](wf)
	if err != nil {
		return err
	}

	// 6.2 Execute：幂等执行。崩溃重放时若日志中已有该位置的 KindExec 记录，则跳过实际执行。
	// WithLabel 附加人类可读标签（仅供调试，不影响重放匹配）。
	greeting, err := engine.Execute(wf, func(ctx context.Context) (string, error) {
		// 这里可以是任意副作用：HTTP 调用、DB 写入等。
		return "Hello, " + in.Name, nil
	}, engine.WithLabel("greet"))
	if err != nil {
		return err
	}

	// 6.3 Sleep：长时等待，不占用 Worker 线程。
	// 引擎捕获 ErrSleeping 并在 deadline 到达时自动唤醒，业务代码只需透传错误。
	if err := wf.Sleep(50*time.Millisecond, engine.WithLabel("nap")); err != nil {
		return err
	}

	// 6.4 Await：跨重启等待外部信号。
	payload, err := wf.Await(model.Signal("approved"), 5*time.Second, engine.WithLabel("approve"))
	if err != nil {
		if engine.IsAwaitTimeout(err) {
			return wf.Return(greeting + " (unapproved)")
		}
		// ErrAwaiting 由引擎捕获并挂起，等待 Signal 唤醒。
		return err
	}
	var approval map[string]string
	_ = json.Unmarshal(payload, &approval)

	// 6.5 Return：立即终止并返回结果。
	return wf.Return(greeting + " approved by " + approval["by"])
}

// wait 轮询直到实例进入终态或超时。
func wait(ctx context.Context, e *engine.Engine, runID model.RunID, d time.Duration) (*model.RunMeta, error) {
	deadline := time.Now().Add(d)
	for {
		m, err := e.GetResult(ctx, runID)
		if err != nil {
			return nil, err
		}
		if m.Status.IsTerminal() {
			return m, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for %s (status=%s)", runID, m.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
