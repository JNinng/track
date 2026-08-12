package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/jninng/track/policy"
	"github.com/jninng/track/store"
)

// newSyncEngine 构造一个同步执行、不自动 Recover 的引擎，便于确定性测试。
func newSyncEngine(t *testing.T, clk clock.Clock) (*Engine, *memory.Store) {
	t.Helper()
	s := memory.New()
	e := NewEngine(s, WithClock(clk))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true
	return e, s
}

// awaitStatus 轮询直到实例进入终态，或超时失败。
func awaitStatus(t *testing.T, e *Engine, runID model.RunID, d time.Duration) *model.RunMeta {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		m, err := e.GetResult(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetResult: %v", err)
		}
		if m.Status.IsTerminal() {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for terminal status, current=%s", m.Status)
		}
		time.Sleep(time.Millisecond)
	}
}

// ---- 基础原语测试 ----

func TestExecuteRecordsAndReplays(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var calls int32
	e.Register("w", func(wf *WorkflowContext) error {
		v, err := Execute(wf, "double", func(ctx context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 21, nil
		})
		if err != nil {
			return err
		}
		if v != 21 {
			return errors.New("unexpected value")
		}
		return wf.Return(v * 2)
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("fn called %d times, want 1", calls)
	}
	// 输出应为 42。
	var out int
	if err := jsonUnmarshal(m.Output, &out); err != nil {
		t.Fatal(err)
	}
	if out != 42 {
		t.Fatalf("output=%d want 42", out)
	}

	// 重放：直接再次运行，fn 不应被再次调用，日志不变。
	golden := mustReadLogs(t, s, rid)
	atomic.StoreInt32(&calls, 0)
	e.run(context.Background(), rid)
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("replay called fn %d times, want 0 (non-idempotent)", calls)
	}
	if !logsEqual(golden, mustReadLogs(t, s, rid)) {
		t.Fatal("replay logs differ from golden logs")
	}
}

func TestInputPrimitive(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	type in struct {
		Name string
		N    int
	}
	e.Register("w", func(wf *WorkflowContext) error {
		v, err := Input[in](wf)
		if err != nil {
			return err
		}
		return wf.Return(v.Name + ":" + itoa(v.N))
	})
	rid, _ := e.Start(context.Background(), "w", in{Name: "x", N: 7})
	m := awaitStatus(t, e, rid, time.Second)
	var got string
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got != "x:7" {
		t.Fatalf("got %q", got)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	var attempts int32
	e.Register("w", func(wf *WorkflowContext) error {
		_, err := Execute(wf, "flaky", func(ctx context.Context) (int, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				return 0, errors.New("transient")
			}
			return 99, nil
		}, WithRetry(policy.NewFixedDelay(0, 3)))
		return err
	})
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, 2*time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
}

func TestLoopStepIDUniqueness(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var calls int32
	e.Register("w", func(wf *WorkflowContext) error {
		for i := 0; i < 5; i++ {
			// 同一调用点在循环中多次调用：StepID 需自动附加序号。
			_, err := Execute(wf, "", func(ctx context.Context) (int, error) {
				atomic.AddInt32(&calls, 1)
				return i, nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	logs := mustReadLogs(t, s, rid)
	if len(logs) != 5 {
		t.Fatalf("want 5 log entries, got %d", len(logs))
	}
	// 校验 5 个 StepID 互不相同。
	seen := map[model.StepID]struct{}{}
	for _, l := range logs {
		if _, dup := seen[l.StepID]; dup {
			t.Fatalf("duplicate StepID %s", l.StepID)
		}
		seen[l.StepID] = struct{}{}
	}
	// 重放：5 次均命中历史，fn 不再调用。
	atomic.StoreInt32(&calls, 0)
	e.run(context.Background(), rid)
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("replay re-executed loop steps: %d calls", calls)
	}
}

func TestVersionMismatchOnReplay(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error { return wf.Return(1) }, WithVersion("v1"))

	rid, _ := e.Start(context.Background(), "w", nil)
	awaitStatus(t, e, rid, time.Second)

	// 伪造历史中的版本号与当前注册版本不一致。
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning,
		store.WithVersion("v_other")); err != nil {
		t.Fatal(err)
	}

	e.run(context.Background(), rid)
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s, want Failed on version mismatch", m.Status)
	}
	if m.Err == "" {
		t.Fatal("expected non-empty error message")
	}
}

// 显式 StepID 在不同调用点被复用时，必须报错而非让第二个调用点静默命中
// 第一个的历史记录（串台）。回归 ErrStepIDCollision。
func TestExecuteRejectsCrossCallSiteStepIDReuse(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		v1, err := Execute(wf, "dup", func(ctx context.Context) (int, error) { return 1, nil })
		if err != nil {
			return err
		}
		// 不同调用点、同一显式 StepID：应返回 ErrStepIDCollision。
		v2, err := Execute(wf, "dup", func(ctx context.Context) (int, error) { return 2, nil })
		if err != nil {
			return err
		}
		return wf.Return(v1 + v2)
	})
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s, want Failed on StepID collision", m.Status)
	}
	// RunMeta.Err 是纯字符串，无法用 errors.Is 还原哨兵链；按消息内容校验。
	if !strings.Contains(m.Err, "duplicate StepID") {
		t.Fatalf("err=%q, want ErrStepIDCollision", m.Err)
	}
}

// 同一显式 StepID 在同一调用点的循环中是合法的（由序号 #N 去重），不应报错。
func TestExecuteLoopWithExplicitStepIDOK(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		for i := 0; i < 3; i++ {
			if _, err := Execute(wf, "iter", func(ctx context.Context) (int, error) { return i, nil }); err != nil {
				return err
			}
		}
		return nil
	})
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	logs := mustReadLogs(t, s, rid)
	if len(logs) != 3 {
		t.Fatalf("want 3 log entries, got %d", len(logs))
	}
	seen := map[model.StepID]struct{}{}
	for _, l := range logs {
		seen[l.StepID] = struct{}{}
	}
	if len(seen) != 3 {
		t.Fatalf("loop StepIDs not unique: %v", seen)
	}
}

// 对不存在的实例发信号应返回错误，且不应持久化孤儿信号。
// 回归：修复前 GetResult 的错误被静默吞掉，Signal 对未知 runID 仍返回 nil。
func TestSignalUnknownRunIDReturnsError(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	rid := model.RunID("does-not-exist")
	err := e.Signal(context.Background(), rid, "sig", "x")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if s.Has(rid, "sig") {
		t.Fatal("signal must not be persisted for unknown runID")
	}
}

// 终态实例发信号应 no-op 成功，且不持久化（信号无意义）。
func TestSignalTerminalInstanceIsNoop(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error { return wf.Return("ok") })
	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("setup failed: %s", m.Status)
	}
	if err := e.Signal(context.Background(), rid, "late", "x"); err != nil {
		t.Fatalf("terminal signal should be noop, got %v", err)
	}
	if s.Has(rid, "late") {
		t.Fatal("terminal signal should not persist")
	}
}
