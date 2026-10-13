package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// waitOutcome 收集一次 WaitDone 调用的结果，供 goroutine 传回测试主协程。
type waitOutcome struct {
	meta *model.RunMeta
	err  error
}

// waitAsync 在独立 goroutine 中调用 WaitDone，返回结果通道（模拟使用方等待）。
func waitAsync(e *Engine, runID model.RunID) <-chan waitOutcome {
	out := make(chan waitOutcome, 1)
	go func() {
		meta, err := e.WaitDone(context.Background(), runID)
		out <- waitOutcome{meta: meta, err: err}
	}()
	return out
}

// assertStillWaiting 断言等待尚未返回（实例仍处于非终态推进中）。
func assertStillWaiting(t *testing.T, out <-chan waitOutcome) {
	t.Helper()
	select {
	case o := <-out:
		t.Fatalf("WaitDone returned early: meta=%v err=%v", o.meta, o.err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestWaitDoneImmediateForTerminal 已终态实例（三种终态）立即返回，不阻塞。
func TestWaitDoneImmediateForTerminal(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	// Succeeded。
	e.Register("ok", func(wf *WorkflowContext) error {
		return wf.Return("done")
	})
	rid, err := e.Start(context.Background(), "ok", nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := e.WaitDone(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s want Succeeded", m.Status)
	}

	// Failed。
	e.Register("bad", func(wf *WorkflowContext) error {
		return errors.New("boom")
	})
	rid2, err := e.Start(context.Background(), "bad", nil)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := e.WaitDone(context.Background(), rid2)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Status != model.StatusFailed || m2.Err != "boom" {
		t.Fatalf("status=%s err=%q, want Failed/boom", m2.Status, m2.Err)
	}

	// Cancelled：挂起后取消，WaitDone 直接读到终态。
	e.Register("held", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), time.Hour)
		return err
	})
	rid3, err := e.Start(context.Background(), "held", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), rid3, WithReason("no")); err != nil {
		t.Fatal(err)
	}
	m3, err := e.WaitDone(context.Background(), rid3)
	if err != nil {
		t.Fatal(err)
	}
	if m3.Status != model.StatusCancelled {
		t.Fatalf("status=%s want Cancelled", m3.Status)
	}
}

// TestWaitDoneBlocksUntilCompletion 阻塞至运行完成；运行中的实例不会提前唤醒。
func TestWaitDoneBlocksUntilCompletion(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newAsyncEngine(t, clk)

	entered := make(chan struct{})
	release := make(chan struct{})
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			close(entered)
			<-release
			return 1, nil
		}); err != nil {
			return err
		}
		return wf.Return("done")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-entered // Worker 已进入业务函数（锁被持有，实例非终态）

	out := waitAsync(e, rid)
	assertStillWaiting(t, out)

	close(release)
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("WaitDone: %v", o.err)
		}
		if o.meta.Status != model.StatusSucceeded {
			t.Fatalf("status=%s want Succeeded", o.meta.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after completion")
	}
}

// TestWaitDoneWakesOnFailure 终态失败同样唤醒等待者（fail 路径的通知）。
func TestWaitDoneWakesOnFailure(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newAsyncEngine(t, clk)

	entered := make(chan struct{})
	release := make(chan struct{})
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			close(entered)
			<-release
			return 0, errors.New("late boom")
		}); err != nil {
			return err
		}
		return wf.Return("done")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-entered

	out := waitAsync(e, rid)
	assertStillWaiting(t, out)

	close(release)
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("WaitDone: %v", o.err)
		}
		if o.meta.Status != model.StatusFailed || o.meta.Err != "late boom" {
			t.Fatalf("status=%s err=%q, want Failed/late boom", o.meta.Status, o.meta.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after failure")
	}
}

// TestWaitDoneNotFound 不存在的实例透传存储的 ErrNotFound。
func TestWaitDoneNotFound(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	_, err := e.WaitDone(context.Background(), "run_missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err=%v want store.ErrNotFound", err)
	}
}

// TestWaitDoneCallerCtxCancel 调用方 ctx 取消解除阻塞，等待者登记随之清理。
func TestWaitDoneCallerCtxCancel(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), 0) // 无超时：仅等信号
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan waitOutcome, 1)
	go func() {
		meta, err := e.WaitDone(ctx, rid)
		out <- waitOutcome{meta: meta, err: err}
	}()

	// 确认已进入等待（登记完成）后再取消，避免取消先于登记的竞态。
	deadline := time.Now().Add(time.Second)
	for {
		if e.waiters.count(rid) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never registered")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case o := <-out:
		if !errors.Is(o.err, context.Canceled) {
			t.Fatalf("err=%v want context.Canceled", o.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after ctx cancel")
	}

	// 登记已清理，不泄漏。
	if n := e.waiters.len(); n != 0 {
		t.Fatalf("waiter registry has %d entries after ctx cancel, want 0", n)
	}
}

// TestWaitDoneUnblocksOnEngineStop 引擎关闭解除等待（返回引擎内部 ctx 错误）。
func TestWaitDoneUnblocksOnEngineStop(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), 0)
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	out := waitAsync(e, rid)
	assertStillWaiting(t, out)

	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case o := <-out:
		if !errors.Is(o.err, context.Canceled) {
			t.Fatalf("err=%v want context.Canceled (engine stopped)", o.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after Stop")
	}
}

// TestWaitDoneWakesOnCancel 取消落盘后唤醒等待者（Cancel 路径的通知）。
func TestWaitDoneWakesOnCancel(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), time.Hour)
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	out := waitAsync(e, rid)
	assertStillWaiting(t, out)

	if err := e.Cancel(context.Background(), rid, WithReason("rejected")); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("WaitDone: %v", o.err)
		}
		if o.meta.Status != model.StatusCancelled {
			t.Fatalf("status=%s want Cancelled", o.meta.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after cancel")
	}
}

// TestWaitDoneIgnoresNonTerminalWakeups 非终态推进（睡眠唤醒后再挂起）不唤醒
// 等待者；只有真正进入终态才返回。
func TestWaitDoneIgnoresNonTerminalWakeups(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(time.Hour, WithLabel("nap")); err != nil {
			return err
		}
		return wf.Return("done")
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	out := waitAsync(e, rid)

	// 推进到睡眠中段：唤醒后再次挂起（非终态），等待者不得返回。
	clk.Advance(30 * time.Minute)
	time.Sleep(20 * time.Millisecond) // 给唤醒 goroutine 调度时间
	assertStillWaiting(t, out)

	// 推进过睡眠 deadline：完成终态，等待者返回。
	clk.Advance(35 * time.Minute)
	select {
	case o := <-out:
		if o.err != nil {
			t.Fatalf("WaitDone: %v", o.err)
		}
		if o.meta.Status != model.StatusSucceeded {
			t.Fatalf("status=%s want Succeeded", o.meta.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitDone did not return after final wakeup")
	}
}

// TestWaitDoneConcurrentWaiters 同一实例的多个等待者全部被唤醒；终态后新的
// WaitDone 立即返回。
func TestWaitDoneConcurrentWaiters(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), time.Hour)
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	const n = 5
	outs := make([]<-chan waitOutcome, n)
	for i := range outs {
		outs[i] = waitAsync(e, rid)
	}
	// 等全部登记完成，避免信号先于部分登记（该情形下晚登记者走立即返回路径，
	// 断言依然成立，但会让「并发唤醒」的覆盖变弱）。
	deadline := time.Now().Add(time.Second)
	for {
		if e.waiters.count(rid) == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d waiters registered", e.waiters.count(rid), n)
		}
		time.Sleep(time.Millisecond)
	}

	if err := e.Signal(context.Background(), rid, model.Signal("go"), nil); err != nil {
		t.Fatal(err)
	}
	for i, out := range outs {
		select {
		case o := <-out:
			if o.err != nil {
				t.Fatalf("waiter %d: %v", i, o.err)
			}
			if o.meta.Status != model.StatusSucceeded {
				t.Fatalf("waiter %d: status=%s want Succeeded", i, o.meta.Status)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return", i)
		}
	}

	// 终态后的新等待：立即返回且登记表为空（notify 已清除）。
	m, err := e.WaitDone(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s want Succeeded", m.Status)
	}
	if left := e.waiters.len(); left != 0 {
		t.Fatalf("waiter registry has %d entries, want 0", left)
	}
}
