package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
)

// 竞态测试（设计文档 12.4）：多个 Worker 竞争同一 RunID，锁的互斥性保证
// 同一时刻只有一个 Worker 进入业务逻辑。
func TestLockMutualExclusion(t *testing.T) {
	clk := clock.NewFakeClock()
	s := memory.New()
	// 异步引擎，多 Worker。
	e := NewEngine(s, WithClock(clk), WithWorkers(4))
	e.testOpts.noAutoRecover = true
	defer e.Stop(context.Background())

	var active int32
	var maxActive int32
	release := make(chan struct{})

	e.Register("w", func(wf *WorkflowContext) error {
		cur := atomic.AddInt32(&active, 1)
		for {
			old := atomic.LoadInt32(&maxActive)
			if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
				break
			}
		}
		// 阻塞持有锁，直到测试放行。
		<-release
		atomic.AddInt32(&active, -1)
		return wf.Return("done")
	})

	rid, _ := e.Start(context.Background(), "w", nil)

	// 等待首个 Worker 进入业务逻辑。
	if !waitForCond(time.Second, func() bool { return atomic.LoadInt32(&active) == 1 }) {
		t.Fatal("workflow did not start")
	}

	// 将同一 RunID 多次重新入队，模拟多个 Worker 竞争。
	for i := 0; i < 8; i++ {
		e.sched.enqueue(rid)
	}
	// 给竞争 Worker 调度机会。
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt32(&maxActive); got != 1 {
		t.Fatalf("max concurrent execution = %d, want 1 (lock violated)", got)
	}

	// 放行，工作流应恰好完成一次。
	close(release)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
}

// 锁租约接管（设计文档 12.4）：模拟 Worker 崩溃（不释放锁），锁超时后
// 后续 Worker 必须能够接管并恢复执行。
func TestLockLeaseTakeover(t *testing.T) {
	clk := clock.NewFakeClock()
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithWorkers(2))
	e.testOpts.noAutoRecover = true
	defer e.Stop(context.Background())

	var done int32
	e.Register("w", func(wf *WorkflowContext) error {
		atomic.AddInt32(&done, 1)
		return wf.Return("ok")
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	// 手动占住锁，模拟一个崩溃的 Worker 持有它且永不释放。
	if ok, _ := s.Acquire(context.Background(), rid); !ok {
		t.Fatal("could not simulate dead worker lock")
	}

	// 再次调度：应因锁被占用而无法进入，状态保持 Awaiting/Running 之外不会完成。
	e.sched.enqueue(rid)
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&done) != 0 {
		t.Fatal("workflow ran while lock held by dead worker")
	}

	// 锁租约过期 -> 后续 Worker 可接管。
	s.Expire(rid)
	e.sched.enqueue(rid)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s after takeover", m.Status, m.Err)
	}
}

// 异步引擎端到端：Start 投递后由 Worker 池完成。
func TestAsyncEngineEndToEnd(t *testing.T) {
	s := memory.New()
	e := NewEngine(s, WithWorkers(2))
	defer e.Stop(context.Background())

	e.Register("w", func(wf *WorkflowContext) error {
		x, err := Execute(wf, "add", func(ctx context.Context) (int, error) { return 40, nil })
		if err != nil {
			return err
		}
		return wf.Return(x + 2)
	})

	rid, _ := e.Start(context.Background(), "w", nil)
	m := awaitStatus(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	var got int
	if err := jsonUnmarshal(m.Output, &got); err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("got %d want 42", got)
	}
}

// waitForCond 轮询 cond，最长等待 d。
func waitForCond(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
