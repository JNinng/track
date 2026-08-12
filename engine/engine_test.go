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
		v, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 21, nil
		}, WithLabel("double"))
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
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
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

// 循环中的幂等步骤：位置重放下每次迭代各占一条 KindExec 条目，
// 重放时按位置逐条命中，fn 不再调用。
func TestLoopPositionalReplay(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var calls int32
	e.Register("w", func(wf *WorkflowContext) error {
		for i := 0; i < 5; i++ {
			_, err := Execute(wf, func(ctx context.Context) (int, error) {
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
	// 位置重放：身份由位置决定，条目 Kind 均为 KindExec。
	for i, l := range logs {
		if l.Kind != model.KindExec {
			t.Fatalf("log[%d].Kind=%s, want %s", i, l.Kind, model.KindExec)
		}
	}
	// 重放：5 次均按位置命中历史，fn 不再调用。
	atomic.StoreInt32(&calls, 0)
	e.run(context.Background(), rid)
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("replay re-executed loop steps: %d calls", calls)
	}
	if !logsEqual(logs, mustReadLogs(t, s, rid)) {
		t.Fatal("replay logs differ from golden logs")
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

// 终态时若历史存在未被消费的残余条目，说明代码路径相较录制时缩短
// （如删除了某个原语调用），引擎必须以 ErrJournalMismatch 失败，而非静默成功。
func TestJournalMismatchOnLeftoverEntries(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		return wf.Return("ok") // 不调用 Execute -> 孤儿条目不会被消费
	})

	// 预置一条孤儿 KindExec 历史，模拟"录制时执行过、当前代码已删除"。
	rid := model.RunID("run-leftover")
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning,
		store.WithName("w"), store.WithVersion(defaultVersion("w"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), rid, model.LogEntry{Kind: model.KindExec, Payload: []byte("1")}); err != nil {
		t.Fatal(err)
	}

	e.run(context.Background(), rid)
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s, want Failed on journal drift", m.Status)
	}
	// RunMeta.Err 是纯字符串，无法用 errors.Is 还原哨兵链；按消息内容校验。
	if !strings.Contains(m.Err, "journal mismatch") {
		t.Fatalf("err=%q, want journal mismatch", m.Err)
	}
}

// 与上一个用例对称：业务函数以 `return nil` 结束（非 Return 原语）时，
// 同样必须触发日志发散检测。回归：曾经 runner 的 nil 返回分支丢弃了
// failOnJournalDrift 的返回值，随后 e.succeed 把 Failed 覆盖为 Succeeded，
// 导致"代码删减步骤"的分歧被静默吞掉。
func TestJournalMismatchOnNilReturnLeftover(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		return nil // 不调用 Execute -> 孤儿条目不会被消费
	})

	rid := model.RunID("run-nil-leftover")
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning,
		store.WithName("w"), store.WithVersion(defaultVersion("w"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), rid, model.LogEntry{Kind: model.KindExec, Payload: []byte("1")}); err != nil {
		t.Fatal(err)
	}

	e.run(context.Background(), rid)
	m, _ := e.GetResult(context.Background(), rid)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s, want Failed on journal drift (nil-return path)", m.Status)
	}
	if !strings.Contains(m.Err, "journal mismatch") {
		t.Fatalf("err=%q, want journal mismatch", m.Err)
	}
}

// Label 是纯可读元数据：重放只按位置 + Kind 匹配。即使当前代码使用了
// 与历史不同的 Label，重放仍命中历史条目、不重新执行、不改写已落盘的 Label。
//
// 这是位置日志相比旧行号 StepID 的核心收益：cosmetic 变更不破坏既有重放。
func TestLabelIsCosmeticAcrossReplay(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var calls int32
	mk := func(label string) func(*WorkflowContext) error {
		return func(wf *WorkflowContext) error {
			v, err := Execute(wf, func(ctx context.Context) (int, error) {
				atomic.AddInt32(&calls, 1)
				return 7, nil
			}, WithLabel(label))
			if err != nil {
				return err
			}
			return wf.Return(v)
		}
	}

	// Run1：label="greet"。
	e.Register("w", mk("greet"))
	rid, _ := e.Start(context.Background(), "w", nil)
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	golden := mustReadLogs(t, s, rid)
	if len(golden) != 1 || golden[0].Kind != model.KindExec || golden[0].Label != "greet" {
		t.Fatalf("golden=%+v", golden)
	}

	// “代码变更”：同一工作流名改用 label="renamed"（版本未变）。
	atomic.StoreInt32(&calls, 0)
	e.Register("w", mk("renamed"))
	// 重置为非终态以允许重新 run。
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning); err != nil {
		t.Fatal(err)
	}
	e.run(context.Background(), rid)

	// 重放命中历史（KindExec 匹配），fn 不再执行；落盘 Label 仍是 "greet"。
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("label change caused re-execution: %d calls", got)
	}
	replay := mustReadLogs(t, s, rid)
	if replay[0].Label != "greet" {
		t.Fatalf("label rewritten to %q; cosmetic change must not rewrite history", replay[0].Label)
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
