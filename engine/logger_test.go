package engine

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
)

// recordingLogger 实现 observ.Logger，记录全部日志供测试断言。
// Enabled 恒 true：测试要验证的是"日志真的发出去了"。
type recordingLogger struct {
	mu   sync.Mutex
	recs []logRecord
}

type logRecord struct {
	level slog.Level
	msg   string
	attrs []slog.Attr
}

func (r *recordingLogger) Enabled(level slog.Level) bool { return true }

func (r *recordingLogger) Log(level slog.Level, msg string, attrs ...slog.Attr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, logRecord{level: level, msg: msg, attrs: append([]slog.Attr(nil), attrs...)})
}

// find 返回第一条 msg 匹配的记录；不存在返回 nil。
func (r *recordingLogger) find(msg string) *logRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.recs {
		if r.recs[i].msg == msg {
			return &r.recs[i]
		}
	}
	return nil
}

// countMsg 统计 msg 匹配的记录条数。
func (r *recordingLogger) countMsg(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for i := range r.recs {
		if r.recs[i].msg == msg {
			n++
		}
	}
	return n
}

func (r *recordingLogger) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = nil
}

// newLoggerEngine 构造注入 recordingLogger 的同步引擎。
func newLoggerEngine(t *testing.T, clk clock.Clock, logger observ.Logger) (*Engine, *memory.Store) {
	t.Helper()
	s := memory.New()
	e := NewEngine(s, WithClock(clk), WithLogger(logger))
	e.testOpts.runSync = true
	e.testOpts.noAutoRecover = true
	return e, s
}

// waitLogCount 轮询直到指定 msg 的日志条数达到 want（配合 FakeClock 的异步唤醒）。
func waitLogCount(t *testing.T, logger *recordingLogger, msg string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if logger.countMsg(msg) >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting %dx %q, got %v", want, msg, logger.countMsg(msg))
		}
		time.Sleep(time.Millisecond)
	}
}

// 生命周期日志：引擎启动/停止、run 启动/挂起/信号等低频事件必须经 observ.Logger
// 输出（Info 级），且不得影响执行语义。
func TestLifecycleLogs(t *testing.T) {
	clk := clock.NewFakeClock()
	logger := &recordingLogger{}
	e, _ := newLoggerEngine(t, clk, logger)

	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(10*time.Second, WithLabel("nap")); err != nil {
			return err
		}
		if _, err := wf.Await("go", 5*time.Second, WithLabel("go")); err != nil {
			return err // ErrAwaiting 由引擎捕获挂起；超时/信号后正常返回
		}
		return nil
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if logger.find("engine: run started") == nil {
		t.Fatal("missing run started log")
	}

	// 推进时钟唤醒 Sleep，进入 Await 挂起（异步唤醒，轮询日志确认）。
	clk.Advance(10 * time.Second)
	waitLogCount(t, logger, "engine: run awaiting", 1)

	// 投递信号：run 消费信号完成，应记录 signal received。
	if err := e.Signal(context.Background(), rid, "go", nil); err != nil {
		t.Fatal(err)
	}
	m := waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusSucceeded {
		t.Fatalf("status=%s err=%s", m.Status, m.Err)
	}
	if logger.find("engine: signal received") == nil {
		t.Fatal("missing signal received log")
	}

	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []string{
		"engine: started",
		"engine: run started",
		"engine: run sleeping",
		"engine: run awaiting",
		"engine: signal received",
		"engine: signal consumed",
		"engine: run completed",
		"engine: stopped",
	} {
		if logger.find(msg) == nil {
			t.Fatalf("missing lifecycle log %q, got %v", msg, logger.msgsOf())
		}
	}
	// 同一实例可多次挂起（Sleep 与 Await）：必须以不同 message 区分，
	// 且顺序为先 sleeping 后 awaiting（仅靠 deadline 区分易误读）。
	all := logger.msgsOf()
	idxSleep, idxAwait := -1, -1
	for i, m := range all {
		switch m {
		case "engine: run sleeping":
			idxSleep = i
		case "engine: run awaiting":
			idxAwait = i
		}
	}
	if idxSleep == -1 || idxAwait == -1 || idxSleep > idxAwait {
		t.Fatalf("suspend messages must appear as sleeping then awaiting, got %v", all)
	}
	// 投递来源：每次执行必须携带 source 属性，且按本场景依次为
	// start（Start 投递）→ timer（FakeClock 推进唤醒）→ signal（Signal 投递）。
	wantSources := []string{"start", "timer", "signal"}
	var gotSources []string
	logger.mu.Lock()
	for i := range logger.recs {
		if logger.recs[i].msg == "engine: run dispatched" {
			for _, a := range logger.recs[i].attrs {
				if a.Key == "source" {
					gotSources = append(gotSources, a.Value.String())
				}
			}
		}
	}
	logger.mu.Unlock()
	if !reflect.DeepEqual(gotSources, wantSources) {
		t.Fatalf("dispatch sources = %v, want %v", gotSources, wantSources)
	}
}

// 信号投递到已终态实例是 no-op：必须输出 Debug 级线索，否则发送方拿到
// nil 却无法得知信号未生效。
func TestSignalIgnoredDebugLog(t *testing.T) {
	clk := clock.NewFakeClock()
	logger := &recordingLogger{}
	e, _ := newLoggerEngine(t, clk, logger)

	e.Register("w", func(wf *WorkflowContext) error {
		return nil
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, e, rid, time.Second) // 已完成（终态）

	if err := e.Signal(context.Background(), rid, "late", nil); err != nil {
		t.Fatal(err)
	}
	rec := logger.find("engine: signal ignored, run already terminal")
	if rec == nil || rec.level != slog.LevelDebug {
		t.Fatalf("ignored signal log = %+v, want Debug level", rec)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 失败事件必须以 Error 级输出（run failed）。
func TestFailureErrorLog(t *testing.T) {
	clk := clock.NewFakeClock()
	logger := &recordingLogger{}
	e, _ := newLoggerEngine(t, clk, logger)

	e.Register("w", func(wf *WorkflowContext) error {
		return errors.New("boom")
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	m := waitFor(t, e, rid, time.Second)
	if m.Status != model.StatusFailed {
		t.Fatalf("status=%s want failed", m.Status)
	}
	rec := logger.find("engine: run failed")
	if rec == nil || rec.level != slog.LevelError {
		t.Fatalf("run failed log = %+v, want Error level", rec)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 重放模式必须输出 Debug 级钩子（journal 非空时锚定 isReplay），
// 供确定性调试（设计文档 12.1）。
func TestReplayDebugLog(t *testing.T) {
	clk := clock.NewFakeClock()
	logger := &recordingLogger{}
	e, s := newLoggerEngine(t, clk, logger)

	e.Register("w", func(wf *WorkflowContext) error {
		if err := wf.Sleep(10*time.Second, WithLabel("nap")); err != nil {
			return err
		}
		return nil
	})

	rid, err := e.Start(context.Background(), "w", nil)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(10 * time.Second)
	waitFor(t, e, rid, time.Second) // 首次执行完成（sleep 命中后正常返回）

	// 模拟崩溃后重启：终态实例重置为 Running 后重放（同 determinism 测试模式）。
	logger.reset()
	if err := s.UpdateStatus(context.Background(), rid, model.StatusRunning); err != nil {
		t.Fatal(err)
	}
	e.run(context.Background(), rid, srcManual)

	rec := logger.find("engine: run replaying")
	if rec == nil || rec.level != slog.LevelDebug {
		t.Fatalf("replaying log = %+v, want Debug level", rec)
	}
	// journal_len 是本次 run 开始前的 journal 快照：Sleep 首挂起后仅 1 条，
	// 重放从 len=1 开始（与 hello 示例的 len=2/3 对应不同挂起结构）。
	var journalLen string
	for _, a := range rec.attrs {
		if a.Key == "journal_len" {
			journalLen = a.Value.String()
		}
	}
	if journalLen != "1" {
		t.Fatalf("replaying journal_len = %q, want 1 (snapshot at run start)", journalLen)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// msgsOf 返回全部 msg（调试输出用）。
func (r *recordingLogger) msgsOf() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.recs))
	for i := range r.recs {
		out = append(out, r.recs[i].msg)
	}
	return out
}

// 默认 logger 快照语义：未注入 WithLogger 时，NewEngine 构造期读取
// observ.DefaultLogger() 并固定——构造前 SetDefaultLogger 的值生效；
// 构造后再替换包级默认不再影响本引擎。未设置时为 NoopLogger（零输出），
// 引擎绝不默认向 slog.Default() 输出（库不应产生意外的 stderr 噪声）。
func TestDefaultLoggerSnapshot(t *testing.T) {
	logger := &recordingLogger{}
	old := observ.SetDefaultLogger(logger)
	defer observ.SetDefaultLogger(old)

	s := memory.New()
	e := NewEngine(s, WithClock(clock.NewFakeClock()))
	e.SetTestOptions(TestOptions{RunSync: true, NoAutoRecover: true})
	defer e.Stop(context.Background())

	e.Register("w", func(wf *WorkflowContext) error { return nil })
	if _, err := e.Start(context.Background(), "w", nil); err != nil {
		t.Fatal(err)
	}
	if logger.find("engine: run started") == nil {
		t.Fatal("engine must snapshot observ.DefaultLogger() at construction: pre-set default logger not used")
	}

	// 构造后再替换包级默认：本引擎已固定快照，继续向构造期 logger 输出、
	// 不受新默认影响（若动态读取默认值，记录会改道 NoopLogger 而 recorder 归零）。
	logger.reset()
	observ.SetDefaultLogger(observ.NoopLogger)
	if _, err := e.Start(context.Background(), "w", nil); err != nil {
		t.Fatal(err)
	}
	if logger.find("engine: run started") == nil {
		t.Fatal("engine snapshot must stay fixed: post-construction SetDefaultLogger must not redirect existing engine logs")
	}
}
