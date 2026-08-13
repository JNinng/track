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
	// 1 fetch + 4 平方 = 5 条步骤日志，再加 Return 的 1 条 KindReturn = 6 条。
	if len(golden) != 6 {
		t.Fatalf("golden logs len = %d, want 6", len(golden))
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
	// 两个实例均以 Return(n) 结束：各记录 1 条 KindReturn，条数相同但 payload 不同
	// （分别为 1 与 2），证明日志对比并非恒真、能够区分不同输入。
	if len(logs1) != 1 || len(logs2) != 1 {
		t.Fatalf("want 1 KindReturn each, got logs1=%d logs2=%d", len(logs1), len(logs2))
	}
	if string(logs1[0].Payload) == string(logs2[0].Payload) {
		t.Fatalf("inputs not distinguished: payload1=%s payload2=%s",
			logs1[0].Payload, logs2[0].Payload)
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

// Return 的终态结果必须以日志记录为准，确保崩溃恢复重放时精确复现（设计文档 6.5）。
//
// 回归：Return 参数若含未被日志覆盖的非确定性源（如本例的递增计数器），在「业务函数
// 已到达 Return、终态 UpdateStatus 尚未落盘」之间崩溃后重放，若不记录 KindReturn，
// 将重新计算参数、产生与首次不同的输出，违反「崩溃后精确复现」契约。记录 KindReturn
// 后，重放以历史 payload 为确定性真相，输出恒等于首次值。
func TestReturnResultIsReplayDeterministic(t *testing.T) {
	clk := clock.NewFakeClock()
	e, s := newSyncEngine(t, clk)

	var execCalls int32
	var retCounter int32 // 模拟 Return 参数中的非确定性源：每次调用递增

	e.Register("w", func(wf *WorkflowContext) error {
		_, err := Execute(wf, func(ctx context.Context) (int, error) {
			atomic.AddInt32(&execCalls, 1)
			return 100, nil
		}, WithLabel("step"))
		if err != nil {
			return err
		}
		// 参数含非确定性源：首次计算得 1，每次重放都会再自增。
		n := atomic.AddInt32(&retCounter, 1)
		return wf.Return(n)
	})

	// ---- 1. 首次执行（录制）----
	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("first run failed: %s %s", m.Status, m.Err)
	}
	var first int
	if err := jsonUnmarshal(m.Output, &first); err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("first output=%d want 1", first)
	}
	golden := mustReadLogs(t, s, rid)
	// [KindExec(step), KindReturn]：步骤记录 + 终态结果记录。
	if len(golden) != 2 || golden[0].Kind != model.KindExec || golden[1].Kind != model.KindReturn {
		t.Fatalf("golden=%+v, want [exec, return]", golden)
	}
	if string(golden[1].Payload) != "1" {
		t.Fatalf("KindReturn payload=%s want 1", golden[1].Payload)
	}

	// ---- 2. 模拟崩溃恢复重放：重置 Execute 副作用计数；终态实例需重置为非终态以强制重放。----
	atomic.StoreInt32(&execCalls, 0)
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning); err != nil {
		t.Fatal(err)
	}
	e.run(context.Background(), rid)

	// Execute 在重放时必须被跳过（命中历史）。
	if got := atomic.LoadInt32(&execCalls); got != 0 {
		t.Fatalf("replay re-executed step: %d calls", got)
	}
	m2 := awaitStatus(t, e, rid, time.Second)
	if m2.Status != model.StatusSucceeded {
		t.Fatalf("replay failed: %s %s", m2.Status, m2.Err)
	}

	// 关键断言：输出必须等于首次记录值（1），而非重算值。
	// retCounter 在重放的业务代码里被自增到 2，若 Return 重新计算参数会得到 2。
	var replayed int
	if err := jsonUnmarshal(m2.Output, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed != 1 {
		t.Fatalf("Return result diverged on replay: got %d want 1 (recorded payload must win)", replayed)
	}
	if got := atomic.LoadInt32(&retCounter); got != 2 {
		t.Fatalf("retCounter=%d want 2 (business code re-ran and incremented)", got)
	}

	// 日志同一性：重放后日志与 golden 完全一致（不多不少）。
	if !logsEqual(golden, mustReadLogs(t, s, rid)) {
		t.Fatal("journal must match golden logs after replay")
	}
}
