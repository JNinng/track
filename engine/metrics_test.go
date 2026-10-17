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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// meterScope 是引擎指标在 OTel 中的 Meter 名（惯例取包导入路径）。
const meterScope = "github.com/jninng/track/engine"

// meterProbe 经 OTel sdk/metric 的 ManualReader 收集引擎指标供测试断言。
// OTel 同步仪表接口是密封的（内嵌未导出标记方法），外部包无法实现——
// 可 fake 的只有 Meter，仪表值断言必须走 sdk 收集：
// Sum 取累计和、Gauge 取最新值、Histogram 取观测次数。
type meterProbe struct {
	reader *sdkmetric.ManualReader
}

// newMeterProbe 构造 ManualReader 支撑的探针，provider 随测试结束关闭。
func newMeterProbe(t *testing.T) (*meterProbe, *sdkmetric.MeterProvider) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &meterProbe{reader: reader}, provider
}

// collect 返回名为 name 的指标的聚合数据；不存在返回 nil。
func (p *meterProbe) collect(t *testing.T, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

// counter 返回计数器当前累计值。otel sdk 的计数器流在首次记录时才物化：
// 指标缺席即「从未记录」，与值 0 同义，计为 0（这些断言的意图是「该路径
// 未触发」，缺席正是其证据；仪表创建集中在 newMetrics，名字漂移由非零
// 断言与示例的对照输出兜底）。
func (p *meterProbe) counter(t *testing.T, name string) float64 {
	t.Helper()
	sum, ok := p.collect(t, name).(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) == 0 {
		return 0
	}
	return float64(sum.DataPoints[0].Value)
}

// gauge 返回最新水位；尚无数据点返回 -1 以区分 0。
func (p *meterProbe) gauge(t *testing.T, name string) float64 {
	t.Helper()
	g, ok := p.collect(t, name).(metricdata.Gauge[int64])
	if !ok || len(g.DataPoints) == 0 {
		return -1
	}
	return float64(g.DataPoints[0].Value)
}

// observed 返回直方图观测次数；尚无数据返回 0。
func (p *meterProbe) observed(t *testing.T, name string) int {
	t.Helper()
	h, ok := p.collect(t, name).(metricdata.Histogram[float64])
	if !ok || len(h.DataPoints) == 0 {
		return 0
	}
	return int(h.DataPoints[0].Count)
}

// newMeterEngine 构造注入 ManualReader 支撑 otel Meter 的同步引擎，
// 返回探针供指标断言。
func newMeterEngine(t *testing.T, clk clock.Clock) (*Engine, *memory.Store, *meterProbe) {
	t.Helper()
	p, provider := newMeterProbe(t)
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithMeter(provider.Meter(meterScope)))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true
	return e, s, p
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
	e, st, p := newMeterEngine(t, clk)

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
		if got := p.counter(t, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	// run 尝试次数：start + timer + signal 共 3 次，各观测一次耗时。
	if got := p.observed(t, "track_run_duration_seconds"); got != 3 {
		t.Errorf("run duration observations = %d, want 3", got)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 失败终局与信号忽略的计数断言。
func TestMetricsFailureAndIgnoredSignal(t *testing.T) {
	clk := clock.NewFakeClock()
	e, _, p := newMeterEngine(t, clk)

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
		if got := p.counter(t, name); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 队列满丢弃与深度水位：使用未启动 Worker 的独立调度器（无消费者），
// 容量 1 的队列第二次 enqueue 必然丢弃，水位保持 1。
func TestMetricsQueueDropAndDepth(t *testing.T) {
	clk := clock.NewFakeClock()
	p, provider := newMeterProbe(t)
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithMeter(provider.Meter(meterScope)), WithWorkers(1), WithQueueSize(1))
	// 独立调度器（不 start）：队列无消费者，计数确定。
	sch := newScheduler(e, 1, 1)

	sch.enqueue(context.Background(), "run_a", srcManual)
	sch.enqueue(context.Background(), "run_b", srcManual) // 队列已满：丢弃。

	if got := p.counter(t, "track_queue_dropped_total"); got != 1 {
		t.Errorf("queue dropped = %v, want 1", got)
	}
	if got := p.gauge(t, "track_queue_depth"); got != 1 {
		t.Errorf("queue depth = %v, want 1", got)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// queue_depth 必须在 Worker 消费后回落：gauge 是「当前深度」的资源状态量，
// 若只在 enqueue 时更新，队列排空后指标停留在旧水位，系统性高估、误导告警。
//
// 用首个任务占住唯一 Worker，保证第二个任务入队后深度确定为 1（排除
// enqueue 的记录与 Worker 抢消费的竞态），再放行验证消费后归零。
func TestMetricsQueueDepthDrainsAfterConsume(t *testing.T) {
	p, provider := newMeterProbe(t)
	s := memory.New()
	e := NewEngine(s, WithClock(clock.NewFakeClock()), WithMeter(provider.Meter(meterScope)), WithWorkers(1))
	e.SetTestOptions(TestOptions{NoAutoRecover: true})
	defer e.Stop(context.Background())

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	e.Register("w", func(wf *WorkflowContext) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
			<-release // 首个任务占住唯一 Worker。
		}
		return nil
	})

	rid1, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-entered // Worker 已进入首个任务（忙碌），后续入队不会被立即消费。

	rid2, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.gauge(t, "track_queue_depth"); got != 1 {
		t.Fatalf("precondition: queue depth = %v, want 1 (rid2 queued behind busy worker)", got)
	}

	close(release) // 放行：Worker 依次完成 rid1、消费 rid2。
	waitStatus(t, e, rid2, model.StatusSucceeded, time.Second)
	waitStatus(t, e, rid1, model.StatusSucceeded, time.Second)
	if got := p.gauge(t, "track_queue_depth"); got != 0 {
		t.Fatalf("queue depth = %v after queue drained, want 0", got)
	}
}
