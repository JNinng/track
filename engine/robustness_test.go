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
	"github.com/jninng/track/store"
)

// appendFailingStore 包装 store.Interface，其 Append 在 fail 为真时返回错误，
// 用于验证 journal 写入失败不会被判定为业务终态失败。
type appendFailingStore struct {
	store.Interface
	fail atomic.Bool
}

func (a *appendFailingStore) Append(ctx context.Context, runID model.RunID, entry model.LogEntry) error {
	if a.fail.Load() {
		return errors.New("disk temporarily unavailable")
	}
	return a.Interface.Append(ctx, runID, entry)
}

// fetchFailingStore 包装 store.Interface，其 Fetch 在 fail 为真时返回
// 非 ErrNotFound 的存储错误，用于验证信箱读取失败不判终态。
type fetchFailingStore struct {
	store.Interface
	fail atomic.Bool
}

func (f *fetchFailingStore) Fetch(ctx context.Context, runID model.RunID, signal model.Signal) ([]byte, error) {
	if f.fail.Load() {
		return nil, errors.New("mailbox temporarily unavailable")
	}
	return f.Interface.Fetch(ctx, runID, signal)
}

// TestPanicMarksFailedNotCrash 验证业务函数 panic 被捕获并转换为该实例的
// StatusFailed，而不是击穿 Worker goroutine 导致进程崩溃。
func TestPanicMarksFailedNotCrash(t *testing.T) {
	e := NewEngine(memory.New(), WithClock(clock.NewFakeClock()))
	e.SetTestOptions(TestOptions{RunSync: true, NoAutoRecover: true})
	e.Register("panic", func(wf *WorkflowContext) error {
		panic("boom")
	})

	rid, err := e.Start(context.Background(), "panic", nil)
	if err != nil {
		t.Fatal(err)
	}

	// RunSync 模式下 Start 同步执行 run：若 panic 未被捕获，本测试进程直接崩溃。
	m := waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%v want Failed", m.Status)
	}
	if !contains(m.Err, "panicked") || !contains(m.Err, "boom") {
		t.Fatalf("err=%q want panic message with cause", m.Err)
	}
}

// contains 是 strings.Contains 的简短别名，便于测试阅读（同 helpers_test 风格）。
func contains(s, sub string) bool { return strings.Contains(s, sub) }

// TestAppendFailureKeepsRunning 验证 journal 写入（Append）瞬时失败时实例
// 保持 Running 而非终态 Failed，且故障恢复后重试幂等成功。
func TestAppendFailureKeepsRunning(t *testing.T) {
	s := &appendFailingStore{Interface: memory.New()}
	s.fail.Store(true) // 首次执行期间模拟存储故障
	e := NewEngine(s, WithClock(clock.NewFakeClock()))
	e.SetTestOptions(TestOptions{RunSync: true, NoAutoRecover: true})

	var calls int32
	e.Register("w", func(wf *WorkflowContext) error {
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 42, nil
		}, WithLabel("step1"))
		return err
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	// 首次执行：业务步骤成功但 Append 失败 → 不得判 Failed。
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusRunning {
		t.Fatalf("status=%v want Running (transient storage failure must not be terminal)", m.Status)
	}
	if m.Err != "" {
		t.Fatalf("err=%q want empty", m.Err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls=%d want 1", got)
	}
	// journal 未写入任何条目。
	if logs := mustReadLogs(t, s.Interface.(*memory.Store), rid); len(logs) != 0 {
		t.Fatalf("logs=%d want 0 (append failed, nothing committed)", len(logs))
	}

	// 故障恢复后重试（模拟 Recover 重推）：条目未写入，重试幂等安全。
	s.fail.Store(false)
	e.RunOnce(context.Background(), rid)
	m = waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%v want Succeeded, err=%q", m.Status, m.Err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls=%d want 2 (step re-executed once after recovery)", got)
	}
	logs := mustReadLogs(t, s.Interface.(*memory.Store), rid)
	if len(logs) != 1 || logs[0].Kind != model.KindExec {
		t.Fatalf("logs=%+v want single KindExec", logs)
	}
}

// TestFetchFailureKeepsRunning 验证 Await 的信箱 Fetch 瞬时失败（非 ErrNotFound）
// 不判终态失败，实例保持可恢复。
func TestFetchFailureKeepsRunning(t *testing.T) {
	s := &fetchFailingStore{Interface: memory.New()}
	s.fail.Store(true) // 首次执行期间模拟信箱故障
	e := NewEngine(s, WithClock(clock.NewFakeClock()))
	e.SetTestOptions(TestOptions{RunSync: true, NoAutoRecover: true})

	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := wf.Await("sig", 0); err != nil {
			return err
		}
		return nil
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch 失败（非 NotFound）→ 不得判 Failed。
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusRunning {
		t.Fatalf("status=%v want Running (fetch failure must not be terminal)", m.Status)
	}
	if m.Err != "" {
		t.Fatalf("err=%q want empty", m.Err)
	}

	// 故障恢复：信号到达后重试成功。
	s.fail.Store(false)
	if err := e.Signal(context.Background(), rid, "sig", "hello"); err != nil {
		t.Fatal(err)
	}
	m = waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%v want Succeeded, err=%q", m.Status, m.Err)
	}
}

// TestVersionEmptyMismatch 验证版本校验为严格比较：元数据缺失版本（空串）
// 同样拒绝重放，而非静默跳过。
func TestVersionEmptyMismatch(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithClock(clock.NewFakeClock()))
	e.SetTestOptions(TestOptions{RunSync: true, NoAutoRecover: true})
	e.Register("w", func(wf *WorkflowContext) error { return nil })

	rid := model.RunID("run_manual")
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning,
		store.WithName("w")); err != nil { // 故意不写 Version
		t.Fatal(err)
	}

	e.RunOnce(context.Background(), rid)
	m := waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%v want Failed", m.Status)
	}
	if !contains(m.Err, ErrVersionMismatch.Error()) {
		t.Fatalf("err=%q want version mismatch", m.Err)
	}
}
