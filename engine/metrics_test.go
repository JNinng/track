package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
)

// memMeter 是内存版 observ.Meter，供测试断言指标计数。
type memMeter struct {
	mu         sync.Mutex
	counters   map[string]*memCounter
	gauges     map[string]*memGauge
	histograms map[string]*memHistogram
}

type memCounter struct {
	mu sync.Mutex
	v  float64
}

func (c *memCounter) Inc()          { c.mu.Lock(); c.v++; c.mu.Unlock() }
func (c *memCounter) Add(v float64) { c.mu.Lock(); c.v += v; c.mu.Unlock() }

type memGauge struct {
	mu sync.Mutex
	v  float64
}

func (g *memGauge) Set(v float64) { g.mu.Lock(); g.v = v; g.mu.Unlock() }
func (g *memGauge) Add(v float64) { g.mu.Lock(); g.v += v; g.mu.Unlock() }

type memHistogram struct {
	mu     sync.Mutex
	values []float64
}

func (h *memHistogram) Observe(v float64) {
	h.mu.Lock()
	h.values = append(h.values, v)
	h.mu.Unlock()
}

func newMemMeter() *memMeter {
	return &memMeter{
		counters:   make(map[string]*memCounter),
		gauges:     make(map[string]*memGauge),
		histograms: make(map[string]*memHistogram),
	}
}

func (m *memMeter) NewCounter(name, help string) observ.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := &memCounter{}
	m.counters[name] = c
	return c
}

func (m *memMeter) NewGauge(name, help string) observ.Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := &memGauge{}
	m.gauges[name] = g
	return g
}

func (m *memMeter) NewHistogram(name, help string, buckets []float64) observ.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := &memHistogram{}
	m.histograms[name] = h
	return h
}

// counter 返回指标当前值；不存在（未注册该名）返回 -1 以区分 0。
func (m *memMeter) counter(name string) float64 {
	m.mu.Lock()
	c, ok := m.counters[name]
	m.mu.Unlock()
	if !ok {
		return -1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

// observed 返回直方图观测次数。
func (m *memMeter) observed(name string) int {
	m.mu.Lock()
	h, ok := m.histograms[name]
	m.mu.Unlock()
	if !ok {
		return -1
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.values)
}

// newMeterEngine 构造注入内存 meter 的同步引擎。
func newMeterEngine(t *testing.T, clk clock.Clock, m observ.Meter) (*Engine, *memory.Store, *memMeter) {
	t.Helper()
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithMeter(m))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true
	return e, s, m.(*memMeter)
}

// waitStatus 轮询直到实例进入指定状态或超时（用于消除异步唤醒的时序竞争）。
func waitStatus(t *testing.T, e *Engine, runID model.RunID, want model.RunStatus, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		m, err := e.GetResult(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetResult: %v", err)
		}
		if m.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for status %s, current=%s", want, m.Status)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitJournalKind 轮询直到 journal 出现指定 Kind 的条目或超时。
func waitJournalKind(t *testing.T, s *memory.Store, runID model.RunID, kind model.EntryKind, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		for _, l := range mustReadLogs(t, s, runID) {
			if l.Kind == kind {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for journal kind %s", kind)
		}
		time.Sleep(time.Millisecond)
	}
}

// 完整生命周期（Execute → Sleep → Await → Return + Signal）的指标断言：
// 分发来源枚举拆名、run 终局、信号闭环、journal 追加、run 耗时观测。
func TestMetricsLifecycle(t *testing.T) {
	clk := clock.NewFakeClock()
	m := newMemMeter()
	e, st, mm := newMeterEngine(t, clk, m)

	e.Register("w", func(wf *WorkflowContext) error {
		if _, err := Execute(wf, func(ctx context.Context) (int, error) {
			return 1, nil
		}, WithLabel("step")); err != nil {
			return err
		}
		if err := wf.Sleep(10*time.Second, WithLabel("nap")); err != nil {
			return err
		}
		if _, err := wf.Await("go", 5*time.Second, WithLabel("go")); err != nil {
			return err
		}
		return wf.Return("done")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	// 等待 timer 唤醒（异步 goroutine）真正走到 Await 挂起（journal 出现
	// await_deadline 条目）再投递信号：否则与 Signal 的同步 run 赛跑，
	// Await 可能直接消费信号而不挂起。不能等 StatusAwaiting——Sleep 挂起同为此状态。
	waitJournalKind(t, st, rid, model.KindAwaitDeadline, time.Second)
	if err := e.Signal(context.Background(), rid, "go", nil); err != nil {
		t.Fatal(err)
	}
	meta := waitFor(t, e, rid, time.Second)
	if meta.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", meta.Status, meta.Err)
	}

	for name, want := range map[string]float64{
		// 分发来源（枚举拆名）：start 1 次、timer 1 次（FakeClock 推进唤醒）、signal 1 次。
		"track_dispatch_start_total":  1,
		"track_dispatch_timer_total":  1,
		"track_dispatch_signal_total": 1,
		// run 终局：挂起 2 次（sleep + await），成功 1 次。
		"track_runs_suspended_total": 2,
		"track_runs_succeeded_total": 1,
		"track_runs_failed_total":    0,
		// 信号闭环：received → consumed。
		"track_signals_received_total": 1,
		"track_signals_consumed_total": 1,
		"track_signals_ignored_total":  0,
		// journal：exec + sleep + await_deadline + await + return = 5 条。
		"track_journal_appended_total": 5,
		// 其余路径未触发。
		"track_lock_contended_total": 0,
		"track_queue_dropped_total":  0,
	} {
		if got := mm.counter(name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	// run 尝试次数：start + timer + signal 共 3 次，各观测一次耗时。
	if got := mm.observed("track_run_duration_seconds"); got != 3 {
		t.Errorf("run duration observations = %d, want 3", got)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 失败终局与信号忽略的计数断言。
func TestMetricsFailureAndIgnoredSignal(t *testing.T) {
	clk := clock.NewFakeClock()
	m := newMemMeter()
	e, _, mm := newMeterEngine(t, clk, m)

	e.Register("w", func(wf *WorkflowContext) error {
		return errors.New("boom")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	meta := waitFor(t, e, rid, time.Second)
	if meta.Status != model.StatusFailed {
		t.Fatalf("status=%s want failed", meta.Status)
	}
	// 终态后投递信号：计数入 ignored，不入 received。
	if err := e.Signal(context.Background(), rid, "late", nil); err != nil {
		t.Fatal(err)
	}

	for name, want := range map[string]float64{
		"track_runs_failed_total":      1,
		"track_runs_succeeded_total":   0,
		"track_signals_ignored_total":  1,
		"track_signals_received_total": 0,
	} {
		if got := mm.counter(name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// gauge 返回当前值；不存在返回 -1。
func (m *memMeter) gauge(name string) float64 {
	m.mu.Lock()
	g, ok := m.gauges[name]
	m.mu.Unlock()
	if !ok {
		return -1
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.v
}

// 队列满丢弃与深度水位：使用未启动 Worker 的独立调度器（无消费者），
// 容量 1 的队列第二次 enqueue 必然丢弃，水位保持 1。
func TestMetricsQueueDropAndDepth(t *testing.T) {
	clk := clock.NewFakeClock()
	m := newMemMeter()
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithMeter(m), WithWorkers(1), WithQueueSize(1))
	// 独立调度器（不 start）：队列无消费者，计数确定。
	sch := newScheduler(e, 1, 1)

	sch.enqueue("run_a", srcManual)
	sch.enqueue("run_b", srcManual) // 队列已满：丢弃。

	if got := m.counter("track_queue_dropped_total"); got != 1 {
		t.Errorf("queue dropped = %v, want 1", got)
	}
	if got := m.gauge("track_queue_depth"); got != 1 {
		t.Errorf("queue depth = %v, want 1", got)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
