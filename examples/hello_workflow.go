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
	"log"
	"time"

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
	store := memory.New()
	e := engine.NewEngine(store, engine.WithWorkers(4), engine.WithClock(clock.RealClock{}))
	defer e.Stop(context.Background())

	// 2. 注册工作流。
	if err := e.Register("HelloWorkflow", HelloWorkflow); err != nil {
		log.Fatal(err)
	}

	// 3. 启动一个新实例。
	runID, err := e.Start(context.Background(), "HelloWorkflow", HelloInput{Name: "World"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("started run:", runID)

	// 工作流会先 Sleep，再 Await 一个 "approved" 信号。稍后我们投递该信号。
	go func() {
		time.Sleep(100 * time.Millisecond)
		if err := e.Signal(context.Background(), runID, "approved", map[string]string{"by": "ops"}); err != nil {
			log.Println("signal error:", err)
		}
	}()

	// 4. 轮询结果（生产环境可用事件 / 回调替代轮询）。
	result, err := wait(context.Background(), e, runID, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("status: %s\n", result.Status)
	fmt.Printf("output: %s\n", string(result.Output))
	if result.Err != "" {
		fmt.Printf("error:  %s\n", result.Err)
	}

	// 5. 读取该实例的全部重放日志并打印。
	logs, err := getReplayLogs(context.Background(), store, runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("replay logs:")
	for i, l := range logs {
		fmt.Printf("  %02d. step=%s payload=%s\n", i+1, l.StepID, string(l.Payload))
	}
}

// getReplayLogs 返回指定运行实例的全部重放日志（按追加顺序）。
// store.Reader 是日志读取的最小接口，任何满足它的后端（如 memory.Store）均可传入。
func getReplayLogs(ctx context.Context, r store.Reader, runID model.RunID) ([]model.LogEntry, error) {
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

	// 6.2 Execute：幂等执行。崩溃重放时若日志中已有该 StepID，则跳过实际执行。
	greeting, err := engine.Execute(wf, "greet", func(ctx context.Context) (string, error) {
		// 这里可以是任意副作用：HTTP 调用、DB 写入等。
		return "Hello, " + in.Name, nil
	})
	if err != nil {
		return err
	}

	// 6.3 Sleep：长时等待，不占用 Worker 线程。
	// 引擎捕获 ErrSleeping 并在 deadline 到达时自动唤醒，业务代码只需透传错误。
	if err := wf.Sleep(50 * time.Millisecond); err != nil {
		return err
	}

	// 6.4 Await：跨重启等待外部信号。
	payload, err := wf.Await(model.Signal("approved"), 5*time.Second)
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
