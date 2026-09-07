package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// Retry API 的行为契约测试（设计文档 5.1「Retry 的终态重置契约」）：
//
//   - 仅 Failed 终态可重试：已成功步骤按位置命中跳过、失败步骤真正重新执行
//     （从失败点续跑），journal 保留且重放一致；
//   - 其余状态（Succeeded/Awaiting 等）与未知实例拒绝；
//   - 版本校验在 Retry 路径照常生效；
//   - 连续失败可多次 Retry。

// TestRetryResumesFromFailure 验证核心语义：Failed 实例 Retry 后从失败点续跑。
//
// step1 首次成功（有日志）、step2 注入失败（无日志）：Retry 后 step1 命中历史
// 零副作用、step2 真正重新执行；完成后 journal 追加 step2 记录，重放一致。
func TestRetryResumesFromFailure(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var step1Calls, step2Calls int32
	failStep2 := true
	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&step1Calls, 1)
			return 1, nil
		}, WithLabel("step1")); err != nil {
			return err
		}
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&step2Calls, 1)
			if failStep2 {
				return 0, errors.New("injected failure")
			}
			return 2, nil
		}, WithLabel("step2"))
		return err
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusFailed || m.Err == "" {
		t.Fatalf("setup: status=%s err=%q, want Failed", m.Status, m.Err)
	}
	// 失败路径只留下 step1 的成功记录（Execute 只记录成功，失败不写日志）。
	if logs := mustReadLogs(t, s, rid); len(logs) != 1 || logs[0].Label != "step1" {
		t.Fatalf("journal after failure = %+v, want single step1 exec", logs)
	}

	// 修复故障后 Retry：step1 命中历史跳过，step2 真正重新执行。
	failStep2 = false
	atomic.StoreInt32(&step1Calls, 0)
	if err := e.Retry(context.Background(), rid); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	m = awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded || m.Err != "" {
		t.Fatalf("after retry: status=%s err=%q, want Succeeded", m.Status, m.Err)
	}
	if got := atomic.LoadInt32(&step1Calls); got != 0 {
		t.Fatalf("step1 re-executed on retry: %d calls (non-idempotent)", got)
	}
	if got := atomic.LoadInt32(&step2Calls); got != 2 {
		t.Fatalf("step2 calls=%d, want 2 (1 failed + 1 retried)", got)
	}
	logs := mustReadLogs(t, s, rid)
	if len(logs) != 2 || logs[1].Label != "step2" {
		t.Fatalf("journal after retry = %+v, want step1+step2 execs", logs)
	}

	// Record & Replay（设计文档 12.1）：重置终态后重放，零副作用、日志一致。
	golden := append([]model.LogEntry(nil), logs...)
	atomic.StoreInt32(&step1Calls, 0)
	atomic.StoreInt32(&step2Calls, 0)
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning); err != nil {
		t.Fatal(err)
	}
	e.run(context.Background(), rid, srcManual)
	if got := atomic.LoadInt32(&step1Calls) + atomic.LoadInt32(&step2Calls); got != 0 {
		t.Fatalf("replay re-executed steps: %d calls (non-idempotent)", got)
	}
	if !logsEqual(golden, mustReadLogs(t, s, rid)) {
		t.Fatal("journal differs from golden logs after replay")
	}
}

// 非 Failed 状态一律拒绝：Succeeded 重放会命中全部历史、复现同一输出，无重跑
// 意义；Awaiting 仍在引擎调度范围内，重置反而制造重复投递。
func TestRetryRejectsNonFailed(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	// Succeeded 终态。
	e.Register("ok", func(wf *WorkflowContext) error { return wf.Return("done") })
	rid, _ := e.Start(context.Background(), "ok", nil)
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusSucceeded {
		t.Fatalf("setup: status=%s", m.Status)
	}
	if err := e.Retry(context.Background(), rid); !errors.Is(err, ErrRunNotFailed) {
		t.Fatalf("retry on Succeeded: want ErrRunNotFailed, got %v", err)
	}

	// Awaiting 非终态（Sleep 挂起，FakeClock 不推进）。
	e.Register("sleepy", func(wf *WorkflowContext) error {
		return wf.Sleep(time.Hour)
	})
	rid2, _ := e.Start(context.Background(), "sleepy", nil)
	if m, _ := e.GetResult(context.Background(), rid2); m.Status != model.StatusAwaiting {
		t.Fatalf("setup: status=%s, want Awaiting", m.Status)
	}
	if err := e.Retry(context.Background(), rid2); !errors.Is(err, ErrRunNotFailed) {
		t.Fatalf("retry on Awaiting: want ErrRunNotFailed, got %v", err)
	}

	// 拒绝不得改变实例状态。
	if m, _ := e.GetResult(context.Background(), rid); m.Status != model.StatusSucceeded {
		t.Fatalf("Succeeded status changed by rejected retry: %s", m.Status)
	}
	if m, _ := e.GetResult(context.Background(), rid2); m.Status != model.StatusAwaiting {
		t.Fatalf("Awaiting status changed by rejected retry: %s", m.Status)
	}
}

// 未知实例返回存储的 ErrNotFound，且不创建元数据。
func TestRetryUnknownRunID(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	rid := model.RunID("does-not-exist")
	if err := e.Retry(context.Background(), rid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := s.GetResult(context.Background(), rid); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("retry must not create meta for unknown runID, got err=%v", err)
	}
}

// Retry 不绕过版本守卫：代码版本相对录制时已变更的实例，重试后仍以
// ErrVersionMismatch 失败，而非用新代码静默重放旧日志。
func TestRetryVersionMismatchStillGuards(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _ := newSyncEngine(t, clk)

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 0, errors.New("boom")
		})
		return err
	}, WithVersion("v1"))
	rid, _ := e.Start(context.Background(), "w", nil)
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusFailed {
		t.Fatalf("setup: status=%s, want Failed", m.Status)
	}

	// 代码升级：同名注册提升版本，Retry 后版本校验必须再次失败。
	e.Register("w", func(wf *WorkflowContext) error { return nil }, WithVersion("v2"))
	if err := e.Retry(context.Background(), rid); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s, want Failed on version mismatch after retry", m.Status)
	}
	// RunMeta.Err 是纯字符串，无法用 errors.Is 还原哨兵链；按消息内容校验。
	if !strings.Contains(m.Err, "version mismatch") {
		t.Fatalf("err=%q, want version mismatch", m.Err)
	}
}

// Retry 可重复使用：故障未修复时重试再次失败，修复后再次重试成功。
func TestRetryFailsAgainCanBeRetriedAgain(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var fail = true
	e.Register("w", func(wf *WorkflowContext) error {
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			if fail {
				return 0, errors.New("still broken")
			}
			return 7, nil
		}, WithLabel("step"))
		return err
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusFailed {
		t.Fatalf("setup: status=%s, want Failed", m.Status)
	}

	// 第一次 Retry：故障仍在，再次失败（journal 仍无条目）。
	if err := e.Retry(context.Background(), rid); err != nil {
		t.Fatalf("Retry #1: %v", err)
	}
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusFailed {
		t.Fatalf("after retry #1: status=%s, want Failed (failure persists)", m.Status)
	}
	if logs := mustReadLogs(t, s, rid); len(logs) != 0 {
		t.Fatalf("journal = %+v, want empty (failed step writes no entry)", logs)
	}

	// 第二次 Retry：修复后成功。
	fail = false
	if err := e.Retry(context.Background(), rid); err != nil {
		t.Fatalf("Retry #2: %v", err)
	}
	if m := awaitStatus(t, e, rid, time.Second); m.Status != model.StatusSucceeded {
		t.Fatalf("after retry #2: status=%s err=%q, want Succeeded", m.Status, m.Err)
	}
}
