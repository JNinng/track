package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// newAsyncEngine 构造异步执行（生产 Worker Pool）、不自动 Recover 的引擎，
// 用于验证 Cancel 与在途 run 的锁互斥。
func newAsyncEngine(t *testing.T, clk clock.Clock) (*Engine, *memory.Store) {
	t.Helper()
	s := memory.New()
	e := NewEngine(s, WithClock(clk))
	e.testOpts.noAutoRecover = true
	return e, s
}

// TestCancelAwaitingRun 取消一个挂起中的实例：终态、原因、审计条目、定时器清理、
// 唤醒与迟到信号不再恢复、journal 重放前缀不变、副作用为 0。
func TestCancelAwaitingRun(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var postAwaitCalls int32
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (string, error) {
			return "step1", nil
		}, WithLabel("step1")); err != nil {
			return err
		}
		if _, err := wf.Await(model.Signal("approved"), time.Hour, WithLabel("approve")); err != nil {
			return err
		}
		// 取消后此处不得再执行。
		atomic.AddInt32(&postAwaitCalls, 1)
		return wf.Return("resumed")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	// RunSync 下 Start 同步执行至挂起：状态应为 Awaiting，且唤醒定时器已注册。
	m, err := e.GetResult(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusAwaiting {
		t.Fatalf("status=%s want Awaiting", m.Status)
	}
	if !clk.HasPendingTimers() {
		t.Fatal("wakeup timer should be pending while awaiting")
	}

	// 取消前的 journal 是重放前缀（golden）。
	golden := mustReadLogs(t, s, rid)
	if len(golden) != 2 || golden[0].Kind != model.KindExec || golden[1].Kind != model.KindAwaitDeadline {
		t.Fatalf("unexpected golden journal: %v", golden)
	}

	if err := e.Cancel(context.Background(), rid, WithReason("审批被拒")); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// 终态与原因：Err 记录取消原因。
	m, err = e.GetResult(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusCancelled {
		t.Fatalf("status=%s want Cancelled", m.Status)
	}
	if m.Err != "审批被拒" {
		t.Fatalf("meta.Err=%q want 取消原因", m.Err)
	}
	// 唤醒定时器应已从调度器注销（FakeClock 内部的 timer 条目要等 Advance
	// 触发才移除，故断言调度器的活跃定时器表而非 HasPendingTimers）。
	e.sched.timersMu.Lock()
	_, timerRegistered := e.sched.timers[rid]
	e.sched.timersMu.Unlock()
	if timerRegistered {
		t.Fatal("wakeup timer should be deregistered after cancel")
	}

	// journal = golden 前缀 + 尾部一条 KindCancel（审计，Payload 为原因 JSON）。
	logs := mustReadLogs(t, s, rid)
	if len(logs) != len(golden)+1 {
		t.Fatalf("journal len=%d want %d", len(logs), len(golden)+1)
	}
	if !logsEqual(golden, logs[:len(golden)]) {
		t.Fatal("journal prefix differs from golden")
	}
	last := logs[len(logs)-1]
	if last.Kind != model.KindCancel {
		t.Fatalf("last entry kind=%s want cancel", last.Kind)
	}
	if last.Err != "" {
		t.Fatalf("KindCancel Err=%q want empty", last.Err)
	}
	var reason string
	if err := jsonUnmarshal(last.Payload, &reason); err != nil {
		t.Fatalf("KindCancel payload not JSON string: %v", err)
	}
	if reason != "审批被拒" {
		t.Fatalf("KindCancel payload=%q want 审批被拒", reason)
	}

	// 推进过 await deadline 并手动重投：终态闸门跳过，无恢复、无新条目、零副作用。
	clk.Advance(2 * time.Hour)
	e.RunOnce(context.Background(), rid)
	if atomic.LoadInt32(&postAwaitCalls) != 0 {
		t.Fatalf("post-await code executed after cancel (%d calls)", postAwaitCalls)
	}
	if !logsEqual(logs, mustReadLogs(t, s, rid)) {
		t.Fatal("journal changed by dispatch after cancel")
	}
	m, _ = e.GetResult(context.Background(), rid)
	if m.Status != model.StatusCancelled {
		t.Fatalf("status after dispatch=%s want Cancelled", m.Status)
	}

	// 迟到信号：终态校验 no-op（不持久化、不唤醒、返回 nil）。
	if err := e.Signal(context.Background(), rid, model.Signal("approved"), nil); err != nil {
		t.Fatalf("late Signal should be a no-op nil, got %v", err)
	}
	if cnt := s.PushCount(rid, model.Signal("approved")); cnt != 0 {
		t.Fatalf("late signal persisted %d times, want 0", cnt)
	}
	m, _ = e.GetResult(context.Background(), rid)
	if m.Status != model.StatusCancelled {
		t.Fatalf("status after late signal=%s want Cancelled", m.Status)
	}
}

// TestCancelSleepSuspendedRun 取消一个 Sleep 挂起的实例：唤醒定时器清除后，
// 推进时间不再恢复执行。
func TestCancelSleepSuspendedRun(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var afterSleep int32
	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(time.Hour, WithLabel("nap")); err != nil {
			return err
		}
		atomic.AddInt32(&afterSleep, 1)
		return wf.Return("done")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), rid); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// 缺省原因：Err 记录 "cancelled"，审计条目 Payload 为空。
	m, err := e.GetResult(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusCancelled || m.Err != "cancelled" {
		t.Fatalf("status=%s err=%q, want Cancelled/cancelled", m.Status, m.Err)
	}
	logs := mustReadLogs(t, s, rid)
	if n := len(logs); n != 2 || logs[0].Kind != model.KindSleep || logs[1].Kind != model.KindCancel {
		t.Fatalf("journal = %d entries, want [sleep, cancel]", n)
	}
	if len(logs[1].Payload) != 0 {
		t.Fatalf("KindCancel payload=%q want empty (no reason given)", logs[1].Payload)
	}

	// 推进过 sleep deadline：不再恢复。
	clk.Advance(2 * time.Hour)
	e.RunOnce(context.Background(), rid)
	if atomic.LoadInt32(&afterSleep) != 0 {
		t.Fatalf("post-sleep code executed after cancel (%d calls)", afterSleep)
	}
	m, _ = e.GetResult(context.Background(), rid)
	if m.Status != model.StatusCancelled {
		t.Fatalf("status=%s want Cancelled", m.Status)
	}
}

// TestCancelIdempotent 重复取消幂等：第二次 no-op，不追加第二条 KindCancel；
// 已终态（Succeeded）实例取消同样 no-op。
func TestCancelIdempotent(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := wf.Await(model.Signal("go"), time.Hour); err != nil {
			return err
		}
		return wf.Return("done")
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), rid, WithReason("r1")); err != nil {
		t.Fatal(err)
	}
	afterFirst := mustReadLogs(t, s, rid)

	// 第二次取消：no-op nil，journal 不变。
	if err := e.Cancel(context.Background(), rid, WithReason("r2")); err != nil {
		t.Fatalf("second Cancel should be a no-op nil, got %v", err)
	}
	if !logsEqual(afterFirst, mustReadLogs(t, s, rid)) {
		t.Fatal("journal changed by idempotent cancel")
	}

	// Succeeded 终态实例：Cancel no-op，不写 KindCancel。
	rid2, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Signal(context.Background(), rid2, model.Signal("go"), nil); err != nil {
		t.Fatal(err)
	}
	awaitStatus(t, e, rid2, time.Second)
	logsBefore := mustReadLogs(t, s, rid2)
	if err := e.Cancel(context.Background(), rid2, WithReason("late")); err != nil {
		t.Fatalf("Cancel on Succeeded should be a no-op nil, got %v", err)
	}
	if !logsEqual(logsBefore, mustReadLogs(t, s, rid2)) {
		t.Fatal("journal changed by cancel on terminal run")
	}
	m, _ := e.GetResult(context.Background(), rid2)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s want Succeeded (unchanged)", m.Status)
	}
}

// TestCancelNotFound 取消不存在的实例：透传存储的 ErrNotFound。
func TestCancelNotFound(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	err := e.Cancel(context.Background(), "run_missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err=%v want store.ErrNotFound", err)
	}
}

// TestCancelBusyWhileExecuting 取消正在执行的实例：per-run 锁被持有，
// 返回 ErrRunBusy、不改状态不写日志；释放后实例正常完成。
func TestCancelBusyWhileExecuting(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newAsyncEngine(t, clk)

	entered := make(chan struct{})
	release := make(chan struct{})
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			close(entered) // 已在 Worker 上执行（此刻锁必然被持有）
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
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow never started executing")
	}

	err = e.Cancel(context.Background(), rid, WithReason("mid-flight"))
	if !errors.Is(err, ErrRunBusy) {
		t.Fatalf("err=%v want ErrRunBusy", err)
	}
	// 拒绝取消不得写任何条目。
	if logs := mustReadLogs(t, s, rid); len(logs) != 0 {
		t.Fatalf("journal has %d entries after rejected cancel, want 0", len(logs))
	}

	close(release)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s, want Succeeded", m.Status, m.Err)
	}
	for _, l := range mustReadLogs(t, s, rid) {
		if l.Kind == model.KindCancel {
			t.Fatal("KindCancel written despite ErrRunBusy")
		}
	}
}

// TestCancelStatusFirstThenAudit 钉住「先状态后审计」顺序：审计 Append 失败时
// 取消终态已生效且 Cancel 不回滚、不报错（仅记日志）。
func TestCancelStatusFirstThenAudit(t *testing.T) {
	clk := clock.NewFakeClock()
	base := memory.New()
	fs := &appendFailingStore{Interface: base}
	e := NewEngine(fs, WithClock(clk))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), time.Hour)
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	// 实例先正常挂起（journal = [await_deadline]），再注入 Append 失败——
	// 精确覆盖「仅取消的审计条目写失败」这一场景。
	fs.fail.Store(true)
	suspended := mustReadLogs(t, base, rid)

	// Append 恒定失败：取消仍生效（终态先行），错误不外抛。
	if err := e.Cancel(context.Background(), rid, WithReason("audit-lost")); err != nil {
		t.Fatalf("Cancel should succeed with status-first ordering, got %v", err)
	}
	m, err := e.GetResult(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != model.StatusCancelled || m.Err != "audit-lost" {
		t.Fatalf("status=%s err=%q, want Cancelled/audit-lost", m.Status, m.Err)
	}
	// 审计条目缺失（Append 被注入失败），journal 停留在挂起时的形状。
	if !logsEqual(suspended, mustReadLogs(t, base, rid)) {
		t.Fatal("journal changed despite injected Append failure")
	}
}

// retryGateStore 包装内存后端，一次性拦截武装后的下一次 UpdateStatus：让 Retry
// 持锁停在「校验通过、重置落盘之前」的精确位置，用于钉住 Retry×Cancel 互斥。
type retryGateStore struct {
	store.Interface
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *retryGateStore) UpdateStatus(ctx context.Context, runID model.RunID, status model.RunStatus, opts ...store.MetaOption) error {
	if g.armed.CompareAndSwap(true, false) {
		g.entered <- struct{}{}
		<-g.release
	}
	return g.Interface.UpdateStatus(ctx, runID, status, opts...)
}

// TestRetryMutexBlocksCancel 回归测试：Retry 的校验+重置必须持锁。若 Retry 不加锁，
// 它在 goroutine 中读到 Failed、被闸门暂停的窗口内，Cancel 会成功落盘 Cancelled，
// 随后 Retry 的过期重置将其覆盖为 Running——一次已成功的取消被静默丢弃。修复后，
// Retry 持锁期间 Cancel 必须得到 ErrRunBusy。
func TestRetryMutexBlocksCancel(t *testing.T) {
	clk := clock.NewFakeClock()
	base := memory.New()
	gate := &retryGateStore{Interface: base, entered: make(chan struct{}), release: make(chan struct{})}
	e := NewEngine(gate, WithClock(clk))
	e.testOpts.noAutoRecover = true

	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 1, nil
		}, WithLabel("step")); err != nil {
			return err
		}
		return errors.New("boom")
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusFailed {
		t.Fatalf("setup: status=%s, want Failed", m.Status)
	}

	// 武装闸门，让 Retry 持锁阻塞在校验通过、重置落盘之前的精确位置。
	gate.armed.Store(true)
	retryErr := make(chan error, 1)
	go func() { retryErr <- e.Retry(context.Background(), rid) }()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("retry never reached the gated reset")
	}

	// Retry 持锁窗口内 Cancel 必须被拒绝。
	if err := e.Cancel(context.Background(), rid, WithReason("race")); !errors.Is(err, ErrRunBusy) {
		t.Fatalf("cancel during retry reset: err=%v want ErrRunBusy", err)
	}

	// 放行 Retry：正常完成重置并重投；实例再次失败（故障仍在）。
	close(gate.release)
	if err := <-retryErr; err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusFailed {
		t.Fatalf("after retry: status=%s, want Failed (failure persists)", m.Status)
	}
	// 取消被拒绝，journal 中不得出现 KindCancel（仍是挂起前的 step1 记录）。
	logs := mustReadLogs(t, base, rid)
	if len(logs) != 1 || logs[0].Kind != model.KindExec {
		for _, l := range logs {
			if l.Kind == model.KindCancel {
				t.Fatal("KindCancel written despite ErrRunBusy")
			}
		}
		t.Fatalf("journal = %+v, want single step1 exec", logs)
	}
}

// TestRetryBusyWhileExecuting Retry 的互斥前置：正在执行的实例（锁被 Worker 持有）
// 返回 ErrRunBusy 且不改状态；旧实现此处返回 ErrRunNotFailed、未与在途 run 互斥。
func TestRetryBusyWhileExecuting(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newAsyncEngine(t, clk)

	entered := make(chan struct{})
	release := make(chan struct{})
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			close(entered) // 已在 Worker 上执行（此刻锁必然被持有）
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
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow never started executing")
	}

	if err := e.Retry(context.Background(), rid); !errors.Is(err, ErrRunBusy) {
		t.Fatalf("retry while executing: err=%v want ErrRunBusy", err)
	}

	close(release)
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%q, want Succeeded", m.Status, m.Err)
	}
}

// TestCancelMetrics 取消成功递增 track_runs_cancelled_total；终态 no-op 不递增。
func TestCancelMetrics(t *testing.T) {
	clk := clock.NewFakeClock()
	meter, provider := newMeterProbe(t)
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithMeter(provider.Meter(meterScope)))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await(model.Signal("go"), time.Hour)
		return err
	})
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(context.Background(), rid); err != nil {
		t.Fatal(err)
	}
	if got := meter.counter(t, "track_runs_cancelled_total"); got != 1 {
		t.Fatalf("track_runs_cancelled_total=%v want 1", got)
	}
	// 终态 no-op：不递增。
	if err := e.Cancel(context.Background(), rid); err != nil {
		t.Fatal(err)
	}
	if got := meter.counter(t, "track_runs_cancelled_total"); got != 1 {
		t.Fatalf("track_runs_cancelled_total=%v want 1 after no-op", got)
	}
}
