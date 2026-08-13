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
)

// ---- Sleep 时间穿越约束（设计文档 12.2）----

func TestSleepIsNonBlocking(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	var done int32
	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(time.Hour); err != nil {
			return err
		}
		atomic.StoreInt32(&done, 1)
		return wf.Return("woke")
	})

	start := time.Now()
	rid, _ := e.Start(context.Background(), "w", nil)
	elapsed := time.Since(start)

	// 非阻塞断言：调用 Sleep(1h) 应立即返回，耗时接近 0ms。
	if elapsed > 50*time.Millisecond {
		t.Fatalf("Sleep blocked worker: elapsed=%v", elapsed)
	}
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusAwaiting {
		t.Fatalf("status=%s, want Awaiting", m.Status)
	}
	if atomic.LoadInt32(&done) != 0 {
		t.Fatal("workflow should not have completed before deadline")
	}
}

func TestSleepWakesAfterAdvance(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	var done int32
	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(time.Hour); err != nil {
			return err
		}
		atomic.StoreInt32(&done, 1)
		return wf.Return("woke")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("want Awaiting, got %s", m.Status)
	}

	// 未到 deadline 前推进一小段，不应唤醒。
	clk.Advance(30 * time.Minute)
	time.Sleep(20 * time.Millisecond) // 给唤醒 goroutine 一点调度时间
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("should still be Awaiting before deadline, got %s", m.Status)
	}

	// 越过 deadline 后才应被消费并完成。
	clk.Advance(31 * time.Minute)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	if atomic.LoadInt32(&done) != 1 {
		t.Fatal("workflow did not complete after deadline")
	}
}

// Sleep 的 deadline 必须被持久化，重放时恢复一致的截止时刻。
func TestSleepDeadlinePersistsAcrossRuns(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(2 * time.Hour); err != nil {
			return err
		}
		return wf.Return("ok")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	golden := mustReadLogs(t, s, rid)
	if len(golden) != 1 {
		t.Fatalf("want 1 sleep log entry, got %d", len(golden))
	}

	// 再次运行（模拟崩溃重启）：deadline 应从历史恢复，不追加新日志。
	e.run(context.Background(), rid)
	replay := mustReadLogs(t, s, rid)
	if !logsEqual(golden, replay) {
		t.Fatal("replay after sleep should not append logs")
	}
}

// Sleep 完成后紧接 Await（无信号）不得触发紧忙循环。
//
// 回归：曾经 nextWakeup 优先返回 sleepDeadline，即使 Sleep 已完成（remaining<=0
// 返回 nil）该字段仍为过期值。引擎据此调用 scheduleWakeup，后者见 remaining<=0
// 立即重投，导致工作流体在 200ms 内被进入十余万次。修复后唤醒时刻按返回的
// 哨兵错误选取（ErrSleeping -> sleepDeadline，ErrAwaiting -> awaitDeadline）。
func TestSleepCompletedThenAwaitNoBusyLoop(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithClock(clock.RealClock{}), WithWorkers(1))
	e.testOpts.noAutoRecover = true
	defer e.Stop(context.Background())

	var entries int32
	e.Register("w", func(wf *WorkflowContext) error {
		atomic.AddInt32(&entries, 1)
		if err := wf.Sleep(time.Millisecond); err != nil {
			return err
		}
		_, err := wf.Await("sig", time.Hour)
		return err
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 给引擎充足时间暴露潜在的紧忙循环。
	time.Sleep(200 * time.Millisecond)

	got := atomic.LoadInt32(&entries)
	m, _ := e.GetResult(context.Background(), rid)
	if got > 50 {
		t.Fatalf("busy loop: workflow body entered %d times in 200ms, want <=50 (status=%s)", got, m.Status)
	}
	if m.Status != model.StatusAwaiting {
		t.Fatalf("status=%s, want Awaiting (suspended on Await)", m.Status)
	}
}

// ---- Await 等待原语（设计文档 6.4）----

func TestAwaitReceivesSignal(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		payload, err := wf.Await("ready", 0)
		if err != nil {
			return err
		}
		var msg string
		if err := jsonUnmarshal(payload, &msg); err != nil {
			return err
		}
		return wf.Return(msg)
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 首次运行应挂起等待。
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("want Awaiting, got %s", m.Status)
	}

	if err := e.Signal(context.Background(), rid, "ready", "hello"); err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got string
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("output=%q want hello", got)
	}
}

// 信号先于 Await 到达也不应丢失（信箱竞态保护）。
func TestAwaitSignalBeforeAwaitNotLost(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		payload, err := wf.Await("ready", 0)
		if err != nil {
			return err
		}
		var msg string
		if err := jsonUnmarshal(payload, &msg); err != nil {
			return err
		}
		return wf.Return(msg)
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 工作流已挂起；直接投递信号。
	e.Signal(context.Background(), rid, "ready", "early")
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got string
	jsonUnmarshal(m.Output, &got)
	if got != "early" {
		t.Fatalf("got %q", got)
	}
}

// Ack 必须在消费成功后调用，信号不应被重复消费。
func TestAwaitAckRemovesSignal(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		payload, err := wf.Await("ready", 0)
		if err != nil {
			return err
		}
		return wf.Return(string(payload))
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	e.Signal(context.Background(), rid, "ready", "once")
	awaitStatus(t, e, rid, time.Second)

	if s.Has(rid, "ready") {
		t.Fatal("signal should be Ack'd after consumption")
	}
	// 重放不应再次“消费”或要求信号存在。
	e.run(context.Background(), rid)
	if s.Has(rid, "ready") {
		t.Fatal("signal reappeared after replay")
	}
}

func TestAwaitTimeout(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await("never", 5*time.Second)
		if errors.Is(err, ErrAwaitTimeout) {
			return wf.Return("timed-out")
		}
		return err
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("want Awaiting, got %s", m.Status)
	}
	// 推进超过超时时刻 -> 唤醒 -> Await 返回 ErrAwaitTimeout。
	clk.Advance(6 * time.Second)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got string
	jsonUnmarshal(m.Output, &got)
	if got != "timed-out" {
		t.Fatalf("got %q", got)
	}
}

// Await 的超时决策必须被持久化：一旦走超时分支，后续重放在相同历史日志下
// 必须复现超时，迟到信号不得改变结果（设计文档 12.1 确定性约束）。
//
// 回归：修复前 Await 超时只返回 ErrAwaitTimeout 而不写决策日志，重放时
// 会重新查询信箱，导致迟到信号被消费、输出由"超时"翻转为信号载荷。
//
// 为避免 RunSync 模式下唤醒 goroutine 的异步时序问题，本测试在超时决策
// 落盘（实例终态）后，手动将状态重置为 Awaiting，再直接同步重放。
func TestAwaitTimeoutDecisionPersists(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		_, err := wf.Await("sig", 5*time.Second)
		if IsAwaitTimeout(err) {
			return wf.Return("timed-out")
		}
		if err != nil {
			return err
		}
		return wf.Return("got-signal")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// Run1：无信号 -> 记录 deadline -> ErrAwaiting -> Awaiting。
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("want Awaiting, got %s", m.Status)
	}
	// Run2：超过 deadline -> Fetch miss -> 超时决策落盘 -> Return("timed-out")。
	clk.Advance(6 * time.Second)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded || string(m.Output) != `"timed-out"` {
		t.Fatalf("after timeout want Succeeded/timed-out, got %s/%s", m.Status, string(m.Output))
	}
	golden := mustReadLogs(t, s, rid)

	// 模拟崩溃重启：将终态重置为 Awaiting，允许重新执行 run。
	if err := s.UpdateStatus(context.Background(), rid, model.StatusAwaiting); err != nil {
		t.Fatal(err)
	}
	// 注入一个"迟到"信号——修复前它会改变重放结果。
	s.Push(context.Background(), rid, "sig", []byte(`"LATE"`))

	// 同步重放：确定性要求结果仍是 timed-out，迟到信号被忽略。
	e.run(context.Background(), rid)
	m2, _ := e.GetResult(context.Background(), rid)
	if string(m2.Output) != `"timed-out"` {
		t.Fatalf("determinism broken: late signal changed output to %q", string(m2.Output))
	}
	// 日志不变：超时决策已记录，重放复现分支、不追加新条目。
	if !logsEqual(golden, mustReadLogs(t, s, rid)) {
		t.Fatal("replay after timeout decision should not append logs")
	}
	// 迟到信号未被消费（Await 在超时分支提前返回，未触达 Fetch/Ack）。
	if !s.Has(rid, "sig") {
		t.Fatal("late signal should remain unconsumed on timeout replay")
	}
}

// Await 在“超时时刻已到且信箱已有信号”的场景下，对 Mailbox.Fetch 只应调用一次。
// 回归：修复前超时判定与取值各 Fetch 一次，对远程/计费后端是双倍读。
func TestAwaitSingleFetchWhenDeadlineReachedWithSignal(t *testing.T) {
	clk := clock.NewFakeClock()
	s := &fetchCountingStore{Interface: memory.New()}
	e := NewEngine(s, WithClock(clk))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true

	e.Register("w", func(wf *WorkflowContext) error {
		payload, err := wf.Await("ready", 5*time.Second)
		if err != nil {
			return err
		}
		return wf.Return(string(payload))
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 首次运行记录 deadline 后挂起。
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusAwaiting {
		t.Fatalf("want Awaiting, got %s", m.Status)
	}
	// 仅统计消费过程的 Fetch。
	atomic.StoreInt32(&s.fetches, 0)

	// 推进超过 deadline，再投递信号：此时 deadline 已到且信号已到。
	clk.Advance(6 * time.Second)
	if err := e.Signal(context.Background(), rid, "ready", "hi"); err != nil {
		t.Fatal(err)
	}

	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	if got := atomic.LoadInt32(&s.fetches); got != 1 {
		t.Fatalf("Fetch called %d time(s), want 1 (single fetch on consume)", got)
	}
}

// Await 成功消费信号后 Mailbox.Ack 失败不得判定工作流失败：
// KindAwait 决策已落盘，重放时命中即返回，Ack 失败只留下孤儿信号。
func TestAwaitAckFailureIsNotFatal(t *testing.T) {
	clk := clock.NewFakeClock()
	s := &ackFailingStore{Interface: memory.New(), ackErr: errors.New("simulated ack failure")}
	e := NewEngine(s, WithClock(clk))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true

	e.Register("w", func(wf *WorkflowContext) error {
		payload, err := wf.Await("ready", 0)
		if err != nil {
			return err
		}
		var msg string
		if err := jsonUnmarshal(payload, &msg); err != nil {
			return err
		}
		return wf.Return(msg)
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if err := e.Signal(context.Background(), rid, "ready", "hi"); err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s, want Succeeded (Ack failure must not be fatal)", m.Status, m.Err)
	}
	var got string
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got != "hi" {
		t.Fatalf("output=%q want hi", got)
	}
}

// ---- Return 原语（设计文档 6.5）----

func TestReturnPrimitive(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		return wf.Return(map[string]int{"a": 1})
	})
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got map[string]int
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got["a"] != 1 {
		t.Fatalf("output=%v", got)
	}
}

// 业务函数返回普通 error 应标记为失败。
func TestWorkflowFailure(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	wantErr := errors.New("boom")
	e.Register("w", func(wf *WorkflowContext) error { return wantErr })
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s want Failed", m.Status)
	}
	if m.Err != "boom" {
		t.Fatalf("err=%q want boom", m.Err)
	}
}
