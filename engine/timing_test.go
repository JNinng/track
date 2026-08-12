package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
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
