package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
)

// 竞态测试（设计文档 12.4）：多个 Worker 竞争同一 RunID，锁的互斥性保证
// 同一时刻只有一个 Worker 进入业务逻辑。
func TestLockMutualExclusion(t *testing.T) {
	clk := clock.NewFakeClock()
	s := memory.New()
	// 异步引擎，多 Worker。
	e := NewEngine(s, WithClock(clk), WithWorkers(4))
	e.testOpts.noAutoRecover = true
	defer e.Stop(context.Background())

	var active int32
	var maxActive int32
	release := make(chan struct{})

	e.Register("w", func(wf *WorkflowContext) error {
		cur := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxActive)
			if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
				break
			}
		}
		// 阻塞持有锁，直到测试放行。
		<-release
		atomic.AddInt32(&active, -1)
		return wf.Return("done")
	})

	rid, _ := e.Start(context.Background(), "w", nil)

	// 等待首个 Worker 进入业务逻辑。
	if !waitForCond(time.Second, func() bool { return atomic.LoadInt32(&active) == 1 }) {
		t.Fatal("workflow did not start")
	}

	// 将同一 RunID 多次重新入队，模拟多个 Worker 竞争。
	for i := 0; i < 8; i++ {
		e.sched.enqueue(rid)
	}
	// 给竞争 Worker 调度机会。
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt32(&maxActive); got != 1 {
		t.Fatalf("max concurrent execution = %d, want 1 (lock violated)", got)
	}

	// 放行，工作流应恰好完成一次。
	close(release)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
}

// 锁租约接管（设计文档 12.4）：模拟 Worker 崩溃（不释放锁），锁超时后
// 后续 Worker 必须能够接管并恢复执行。
func TestLockLeaseTakeover(t *testing.T) {
	clk := clock.NewFakeClock()
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithWorkers(2))
	e.testOpts.noAutoRecover = true
	defer e.Stop(context.Background())

	var done int32
	e.Register("w", func(wf *WorkflowContext) error {
		atomic.AddInt32(&done, 1)
		return wf.Return("ok")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 手动占住锁，模拟一个崩溃的 Worker 持有它且永不释放。
	if ok, _ := s.Acquire(context.Background(), rid); !ok {
		t.Fatal("could not simulate dead worker lock")
	}

	// 再次调度：应因锁被占用而无法进入，状态保持 Awaiting/Running 之外不会完成。
	e.sched.enqueue(rid)
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&done) != 0 {
		t.Fatal("workflow ran while lock held by dead worker")
	}

	// 锁租约过期 -> 后续 Worker 可接管。
	s.Expire(rid)
	e.sched.enqueue(rid)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s after takeover", m.Status, m.Err)
	}
}

// 异步引擎端到端：Start 投递后由 Worker 池完成。
func TestAsyncEngineEndToEnd(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithWorkers(2))
	defer e.Stop(context.Background())

	e.Register("w", func(wf *WorkflowContext) error {
		x, err := Execute(wf, func(ctx context.Context) (int, error) { return 40, nil }, WithLabel("add"))
		if err != nil {
			return err
		}
		return wf.Return(x + 2)
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got int
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d want 42", got)
	}
}

// waitForCond 轮询 cond，最长等待 d。
func waitForCond(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// Stop 必须以传入的 ctx 控制关闭超时：当业务 fn 忽略 context 仍阻塞时，
// Stop(ctx) 应在 ctx 到期时返回其错误，而非无限等待 worker 退出。
func TestStopRespectsContextDeadline(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithWorkers(1))
	e.testOpts.noAutoRecover = true

	var entered int32
	release := make(chan struct{})
	e.Register("w", func(wf *WorkflowContext) error {
		atomic.StoreInt32(&entered, 1)
		<-release // 故意忽略 context，模拟不可中断的业务逻辑。
		return wf.Return("ok")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if !waitForCond(time.Second, func() bool { return atomic.LoadInt32(&entered) == 1 }) {
		t.Fatal("workflow did not enter body")
	}

	// ctx 远短于 worker 阻塞时长；Stop 必须据此提前返回。
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := e.Stop(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop err=%v, want DeadlineExceeded (elapsed=%v)", err, elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop did not respect ctx deadline: elapsed=%v", elapsed)
	}

	// 放行被阻塞的 worker，避免 goroutine 泄漏。
	close(release)
	_ = rid
}

// Stop 与并发 Start/唤醒定时器之间不应触发 "send on closed channel" panic。
//
// 回归 scheduler.enqueue 的发送与 stop 的 close 必须互斥：修复前 enqueue 在
// 释放 stopMu 后才向 queue 发送，stop 可能在此间隙 close(queue)。
// 反复运行以放大竞态窗口。
func TestStopConcurrentDispatchNoPanic(t *testing.T) {
	for i := 0; i < 200; i++ {
		s := memory.New()
		e := NewEngine(s, WithClock(clock.RealClock{}), WithWorkers(2))
		e.testOpts.noAutoRecover = true

		e.Register("w", func(wf *WorkflowContext) error {
			if err := wf.Sleep(time.Millisecond); err != nil {
				return err
			}
			return wf.Return("ok")
		})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Stop(context.Background())
		}()
		_, _ = e.Start(context.Background(), "w", nil)
		wg.Wait()
	}
}

// 优雅关停（内部 ctx 取消）期间被中断的工作流不得被标记为 Failed。
//
// 回归：executeWithPolicy 在引擎 ctx 取消时返回 context.Canceled，runner 的 default
// 分支据此调用 fail() 判定 StatusFailed——而 Failed 是终态，Recover 跳过，造成在途
// 工作流永久丢失。这与进程崩溃的语义不对称：崩溃时状态停留在 Running（run 循环第 6
// 步），重启后 Recover 按 journal 幂等重放即可恢复。优雅关停不应比崩溃更致命。
//
// 修复后：当 run 的 ctx 已取消（引擎关停）且业务返回非哨兵错误时，runner 保持 Running，
// 交由重启后 Recover 重新调度。已提交的 journal 步骤在重放时命中、跳过实际副作用，
// 故不会重复执行被中断的那一步。
func TestShutdownInterruptedWorkflowStaysRunning(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithWorkers(1))
	e.testOpts.noAutoRecover = true

	gate := make(chan struct{})
	var entered int32
	e.Register("w", func(wf *WorkflowContext) error {
		// 第一步：成功提交（落入 journal）。
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 1, nil
		}); err != nil {
			return err
		}
		atomic.StoreInt32(&entered, 1)
		<-gate // 阻塞，模拟业务仍在执行。
		// 第二步：引擎 ctx 已取消，executeWithPolicy 感知到并返回 ctx 错误。
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 2, nil
		})
		return err
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 等待第一步提交、业务阻塞在 gate。
	if !waitForCond(time.Second, func() bool { return atomic.LoadInt32(&entered) == 1 }) {
		t.Fatal("workflow did not reach gate")
	}

	// 取消引擎内部 ctx，模拟 Stop 的第一步（e.cancel）。
	e.cancel()
	// 放行：第二步 Execute 将因 ctx 取消返回错误。
	close(gate)
	// 以锁释放为 run 完成信号（run 的 defer 释放锁）。
	if !waitForCond(time.Second, func() bool { return !s.IsLocked(rid) }) {
		t.Fatal("run did not complete (lock not released)")
	}

	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusRunning {
		t.Fatalf("status=%s err=%q, want Running (shutdown-interrupted workflow must stay recoverable, not Failed)",
			m.Status, m.Err)
	}
}

// 被关停中断后保留 Running 的实例，重启后由 Recover 按 journal 幂等重放，
// 完成未执行的那一步——证明"保持 Running"是可恢复的，而非永久滞留。
func TestShutdownInterruptedWorkflowRecoversOnRestart(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithWorkers(1))
	e.testOpts.noAutoRecover = true

	gate := make(chan struct{})
	var entered int32
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 1, nil
		}); err != nil {
			return err
		}
		atomic.StoreInt32(&entered, 1)
		<-gate
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 2, nil
		})
		if err != nil {
			return err
		}
		return wf.Return("done")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if !waitForCond(time.Second, func() bool { return atomic.LoadInt32(&entered) == 1 }) {
		t.Fatal("workflow did not reach gate")
	}
	e.cancel()
	close(gate)
	if !waitForCond(time.Second, func() bool { return !s.IsLocked(rid) }) {
		t.Fatal("run did not complete")
	}
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusRunning {
		t.Fatalf("precondition: status=%s, want Running after shutdown interrupt", m.Status)
	}

	// "重启"：新引擎共享同一存储，手动 Recover 触发重放。重放时第一步命中 journal
	// 跳过，第二步重新执行并提交，最终 Return 完成——证明"保持 Running"是可恢复的。
	e2 := NewEngine(s, WithWorkers(1))
	e2.testOpts.noAutoRecover = true
	e2.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 1, nil
		}); err != nil {
			return err
		}
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 2, nil
		}); err != nil {
			return err
		}
		return wf.Return("done")
	})
	e2.Recover(context.Background())

	m := awaitStatus(t, e2, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%q, want Succeeded after recover-replay", m.Status, m.Err)
	}
}
