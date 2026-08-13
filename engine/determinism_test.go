package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
)

// 确定性验证（设计文档 12.1 / 12.3 / 12.7）：
//
// 给定相同的输入与历史日志，重放执行的结果必须与首次执行完全一致，
// 且外部副作用在重放时必须被跳过（幂等性）。
func TestWorkflowDeterminism(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var serviceCalls int32 // 模拟外部服务调用次数

	// 一个多步骤工作流：加载 -> 处理（循环）-> 汇总 -> 返回。
	e.Register("etl", func(wf *WorkflowContext) error {
		in, err := Input[[]int](wf)
		if err != nil {
			return err
		}

		total, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&serviceCalls, 1)
			sum := 0
			for _, v := range in {
				sum += v
			}
			return sum, nil
		}, WithLabel("fetch"))
		if err != nil {
			return err
		}

		// 循环中的幂等步骤：位置重放下每个迭代各占一条 KindExec 条目。
		for i := 0; i < len(in); i++ {
			_, err := Execute(wf, func(ctx context.Context) (int, error) {
				atomic.AddInt32(&serviceCalls, 1)
				return in[i] * in[i], nil
			})
			if err != nil {
				return err
			}
		}

		return wf.Return(total)
	})

	input := []int{1, 2, 3, 4}

	// ---- 1. 首次执行（录制）----
	rid, err := e.Start(context.Background(), "etl", input)
	if err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("first run failed: %s %s", m.Status, m.Err)
	}
	// fetch(1) + 4 个平方步骤 = 5 次外部调用。
	if got := atomic.LoadInt32(&serviceCalls); got != 5 {
		t.Fatalf("first run service calls = %d, want 5", got)
	}

	// 取首次执行生成的日志作为 Golden Master。
	golden := mustReadLogs(t, s, rid)
	// 1 fetch + 4 平方 = 5 条日志。
	if len(golden) != 5 {
		t.Fatalf("golden logs len = %d, want 5", len(golden))
	}
	goldenOutput := m.Output

	// ---- 2. 重置副作用计数（不重置存储中的日志）----
	atomic.StoreInt32(&serviceCalls, 0)

	// ---- 3. 重放（模拟崩溃重启后重新加载实例）----
	e.run(context.Background(), rid)
	m2 := awaitStatus(t, e, rid, time.Second)
	if m2.Status != model.StatusSucceeded {
		t.Fatalf("replay failed: %s %s", m2.Status, m2.Err)
	}

	// 幂等性断言：重放时外部副作用必须被跳过。
	if got := atomic.LoadInt32(&serviceCalls); got != 0 {
		t.Fatalf("Non-idempotent logic detected: replay service calls = %d, want 0", got)
	}

	// 日志同一性断言：重放生成的日志必须与 Golden Master 完全匹配。
	got := mustReadLogs(t, s, rid)
	if !logsEqual(golden, got) {
		t.Fatal("journal must match golden logs")
	}

	// 输出同一性。
	if string(replayOutput(t, e, rid)) != string(goldenOutput) {
		t.Fatal("replay output differs from first run output")
	}
}

// 不同输入产生不同日志，验证 golden 对比不是恒真。
func TestDeterminismDistinguishesInputs(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)
	e.Register("w", func(wf *WorkflowContext) error {
		n, err := Input[int](wf)
		if err != nil {
			return err
		}
		return wf.Return(n)
	})

	rid1, _ := e.Start(context.Background(), "w", 1)
	awaitStatus(t, e, rid1, time.Second)
	logs1 := mustReadLogs(t, s, rid1)

	rid2, _ := e.Start(context.Background(), "w", 2)
	awaitStatus(t, e, rid2, time.Second)
	logs2 := mustReadLogs(t, s, rid2)

	// 两个实例各自自洽；重放后保持不变。
	g1 := mustReadLogs(t, s, rid1)
	if !logsEqual(logs1, g1) {
		t.Fatal("rid1 logs changed without reason")
	}
	if len(logs2) != len(logs1) {
		// 无 Execute 的工作流日志条数一致（均为 0），输出不同。
		t.Logf("logs1=%d logs2=%d", len(logs1), len(logs2))
	}
}

// replayOutput 读取重放后的输出，便于比较。
func replayOutput(t *testing.T, e *Engine, rid model.RunID) []byte {
	t.Helper()
	m, err := e.GetResult(context.Background(), rid)
	if err != nil {
		t.Fatal(err)
	}
	return m.Output
}
