// Package engine 实现确定性工作流引擎的核心（设计文档第 5 节）。
//
// 引擎采用日志驱动架构：工作流的每一次关键动作都会生成不可变的日志记录，
// 进程崩溃重启后可依据日志完全一致地重放。
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/jninng/observ"
	"github.com/jninng/track/clock"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// Engine 是工作流引擎的顶层句柄。
type Engine struct {
	store     store.Interface
	registry  map[string]*registeredWorkflow
	clock     clock.Clock
	workers   int
	queueSize int

	// recoverInterval 是周期性后台 Recover 扫描的间隔；<=0 表示禁用。
	recoverInterval time.Duration

	// logger 是观测日志出口（observ.Logger），仅用于低频生命周期事件
	// （observ 接入规范第 5 条）。NewEngine 构造期读取 observ.DefaultLogger()
	// 并固定（快照语义），可用 WithLogger 注入其它实现。
	logger observ.Logger

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	sched  *scheduler

	recoverOnce sync.Once
	testOpts    testOptions
}

// NewEngine 创建并启动引擎。store 为 nil 时使用内存后端需由调用方保证非空。
func NewEngine(s store.Interface, opts ...EngineOption) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		store:           s,
		registry:        make(map[string]*registeredWorkflow),
		clock:           clock.RealClock{},
		workers:         defaultWorkers,
		recoverInterval: 30 * time.Second,
		logger:          observ.NewSlogLogger(slog.Default()),
		ctx:             ctx,
		cancel:          cancel,
	}
	for _, o := range opts {
		o(e)
	}
	e.sched = newScheduler(e, e.workers, e.queueSize)
	e.sched.start()
	// 构造期配置快照（低频生命周期事件，进程一次）：workers/queue_size 取
	// 调度器生效值（newScheduler 已按默认逻辑补全）。
	logWith(e.logger, slog.LevelInfo, "engine: started",
		slog.Int("workers", e.sched.workers),
		slog.Int("queue_size", cap(e.sched.queue)),
		slog.Int64("recover_interval_seconds", int64(e.recoverInterval/time.Second)))
	if e.recoverInterval > 0 {
		go e.recoverLoop()
	}
	return e
}

// logWith 是引擎内部统一的日志出口：经由 observ.Logger 输出结构化日志，
// 并 recover 用户 Logger 回调的 panic（observ 接入规范第 3、6 条：回调 panic
// 必须被业务库兜住，观测为纯旁路，不得改变业务执行语义）。
func logWith(l observ.Logger, level slog.Level, msg string, attrs ...slog.Attr) {
	defer func() { _ = recover() }()
	l.Log(level, msg, attrs...)
}

// Register 注册一个工作流。同名注册会覆盖。
func (e *Engine) Register(name string, fn WorkflowFunc, opts ...RegisterOption) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	reg := &registeredWorkflow{name: name, fn: fn, version: defaultVersion(name)}
	for _, o := range opts {
		o(reg)
	}
	e.registry[name] = reg
	return nil
}

// Start 启动一个新的工作流实例（设计文档 5.1 节）。
//
// 仅持久化元数据并将 RunID 推入队列后立即返回；实际执行由 Worker 异步完成。
// 首次调用时（且工作流注册已完成）会自动触发一次 Recover 扫描。
func (e *Engine) Start(ctx context.Context, name string, input any) (model.RunID, error) {
	reg, err := e.lookup(name)
	if err != nil {
		return "", err
	}

	inputBytes, err := marshalInput(input)
	if err != nil {
		return "", err
	}

	runID := newRunID()
	if err := e.store.UpdateStatus(ctx, runID, model.StatusRunning,
		store.WithName(name),
		store.WithVersion(reg.version),
		store.WithInput(inputBytes),
	); err != nil {
		return "", err
	}

	// 实例已持久化：记录生命周期事件（run 时间线锚点，低频）。
	logWith(e.logger, slog.LevelInfo, "engine: run started",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String(observ.AttrWorkflow, name))

	// 首次 Start 自动触发 Recover 扫描（恢复因崩溃遗留的任务）。
	// 排除本次刚创建的实例：它随后的 dispatch 会投递，避免同实例双投递。
	e.recoverOnce.Do(func() {
		if !e.testOpts.noAutoRecover {
			if rerr := e.recoverExcept(ctx, runID); rerr != nil {
				logWith(e.logger, slog.LevelWarn, "engine: auto recover failed",
					slog.String(observ.AttrRunID, string(runID)), slog.Any("error", rerr))
			}
		}
	})

	e.dispatch(runID, srcStart)
	return runID, nil
}

// Signal 向挂起的实例发送信号，触发恢复（设计文档 5.1 节）。
//
// 先校验实例存在且非终态，再持久化信号并唤醒，避免向不存在的实例写入
// 孤儿信号。终态实例视为 no-op（信号无意义，直接返回 nil）。
func (e *Engine) Signal(ctx context.Context, runID model.RunID, signal model.Signal, payload any) error {
	// 先确认实例存在；不存在时返回错误，避免静默成功与孤儿信号。
	m, err := e.store.GetResult(ctx, runID)
	if err != nil {
		return err
	}
	if m.Status.IsTerminal() {
		// 已终态：信号无意义，no-op。Debug 级记录——外部发送方拿到 nil 但信号
		// 未生效时，这是唯一的排查线索（如工作流已超时完成、信号晚到）。
		if e.logger.Enabled(slog.LevelDebug) {
			logWith(e.logger, slog.LevelDebug, "engine: signal ignored, run already terminal",
				slog.String(observ.AttrRunID, string(runID)),
				slog.String("signal", string(signal)))
		}
		return nil // 已终态：信号无意义，no-op。
	}

	var pb []byte
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		pb = b
	}
	if err := e.store.Push(ctx, runID, signal, pb); err != nil {
		return err
	}
	// API 事件：信号已持久化并唤醒实例（低频）。消费侧见 Await 的
	// signal consumed——received 后无 consumed 即信号滞留/丢失。
	logWith(e.logger, slog.LevelInfo, "engine: signal received",
		slog.String(observ.AttrRunID, string(runID)),
		slog.String("signal", string(signal)))
	e.dispatch(runID, srcSignal)
	return nil
}

// GetResult 查询运行结果。
func (e *Engine) GetResult(ctx context.Context, runID model.RunID) (*model.RunMeta, error) {
	return e.store.GetResult(ctx, runID)
}

// Stop 优雅关闭引擎，等待 Worker 停止与活跃定时器取消。
//
// ctx 控制关闭的最长等待时间：若业务 fn 忽略传入的 context 而持续阻塞，
// Stop 会在 ctx 到期时返回其错误，而非无限等待。无论是否超时，活跃的唤醒
// 定时器都会被取消。
func (e *Engine) Stop(ctx context.Context) error {
	e.cancel()
	err := e.sched.stop(ctx)
	if err != nil {
		logWith(e.logger, slog.LevelWarn, "engine: stop incomplete, workers still running",
			slog.Any("error", err))
	} else {
		logWith(e.logger, slog.LevelInfo, "engine: stopped")
	}
	return err
}

// Recover 扫描存储中处于 Running/Awaiting 的实例，重新推入队列恢复执行
// （设计文档第 8 节“持久化兜底机制”）。
func (e *Engine) Recover(ctx context.Context) error {
	return e.recoverExcept(ctx, "")
}

// recoverExcept 同 Recover，但跳过 exclude 指定的实例（空值表示不排除）。
func (e *Engine) recoverExcept(ctx context.Context, exclude model.RunID) error {
	metas, err := e.store.ListByStatus(ctx, model.StatusRunning, model.StatusAwaiting)
	if err != nil {
		return err
	}
	dispatched := 0
	for _, m := range metas {
		if m.RunID == exclude {
			continue
		}
		e.dispatch(m.RunID, srcRecover)
		dispatched++
	}
	// 兜底扫描活动（30s 一次）：有实例被重新投递时 Info（运维可见）；
	// 例行空扫仅 Debug（Enabled 门控，零构造成本）。
	if dispatched > 0 {
		logWith(e.logger, slog.LevelInfo, "engine: recover scan dispatched runs",
			slog.Int("dispatched", dispatched))
	} else if e.logger.Enabled(slog.LevelDebug) {
		logWith(e.logger, slog.LevelDebug, "engine: recover scan dispatched runs",
			slog.Int("dispatched", dispatched))
	}
	return nil
}

// recoverLoop 周期性扫描存储，恢复因崩溃遗留或队列满被丢弃而滞留的实例。
//
// 作为持久化兜底：被丢弃的任务保持 Running/Awaiting 状态，会在下一次扫描时
// 被重新推入队列。间隔由 WithRecoverInterval 配置；引擎关闭（ctx 取消）时退出。
func (e *Engine) recoverLoop() {
	ticker := time.NewTicker(e.recoverInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			if err := e.Recover(e.ctx); err != nil {
				if e.ctx.Err() != nil {
					return // 关闭中：静默退出。
				}
				logWith(e.logger, slog.LevelWarn, "engine: periodic recover failed", slog.Any("error", err))
			}
		}
	}
}

// dispatch 将 RunID 投递给执行路径：同步模式下直接 run，否则进入调度器队列。
//
// source 标识本次投递的来源（见 dispatch source 常量），纯观测元数据：
// 只进日志、不落盘、不参与重放匹配，对确定性零影响。
func (e *Engine) dispatch(runID model.RunID, source string) {
	if e.testOpts.runSync {
		e.run(e.ctx, runID, source)
		return
	}
	e.sched.enqueue(runID, source)
}

// dispatch source 常量（闭合枚举，纯日志元数据）。
const (
	srcStart   = "start"   // Start API 新建实例后的首次投递
	srcSignal  = "signal"  // Signal API 投递信号后的唤醒投递
	srcTimer   = "timer"   // 唤醒定时器到期（sleep/await deadline）
	srcRecover = "recover" // Recover 扫描（首次自动 + 周期兜底）
	srcManual  = "manual"  // RunOnce 手动/测试触发
)

// newRunID 生成一个基于随机字节的十六进制 RunID。
func newRunID() model.RunID {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// 退化方案：用时间戳保证唯一性。
		return model.RunID(hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000"))))
	}
	return model.RunID("run_" + hex.EncodeToString(b))
}

// marshalInput 序列化输入参数；nil 视为空输入。
func marshalInput(input any) ([]byte, error) {
	if input == nil {
		return nil, nil
	}
	return json.Marshal(input)
}
