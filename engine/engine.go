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
	"log"
	"sync"
	"time"

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
		store:    s,
		registry: make(map[string]*registeredWorkflow),
		clock:    clock.RealClock{},
		workers:  defaultWorkers,
		ctx:      ctx,
		cancel:   cancel,
	}
	for _, o := range opts {
		o(e)
	}
	e.sched = newScheduler(e, e.workers, e.queueSize)
	e.sched.start()
	return e
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

	// 首次 Start 自动触发 Recover 扫描（恢复因崩溃遗留的任务）。
	e.recoverOnce.Do(func() {
		if !e.testOpts.noAutoRecover {
			if rerr := e.Recover(ctx); rerr != nil {
				log.Printf("engine: auto recover failed: %v", rerr)
			}
		}
	})

	e.dispatch(runID)
	return runID, nil
}

// Signal 向挂起的实例发送信号，触发恢复（设计文档 5.1 节）。
func (e *Engine) Signal(ctx context.Context, runID model.RunID, signal model.Signal, payload any) error {
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
	// 仅在实例非终态时唤醒；Worker 侧也会再次校验状态。
	if m, err := e.store.GetResult(ctx, runID); err == nil && !m.Status.IsTerminal() {
		e.dispatch(runID)
	}
	return nil
}

// GetResult 查询运行结果。
func (e *Engine) GetResult(ctx context.Context, runID model.RunID) (*model.RunMeta, error) {
	return e.store.GetResult(ctx, runID)
}

// Stop 优雅关闭引擎，等待 Worker 停止与活跃定时器取消。
func (e *Engine) Stop(ctx context.Context) error {
	e.cancel()
	e.sched.stop()
	return nil
}

// Recover 扫描存储中处于 Running/Awaiting 的实例，重新推入队列恢复执行
// （设计文档第 8 节“持久化兜底机制”）。
func (e *Engine) Recover(ctx context.Context) error {
	metas, err := e.store.ListByStatus(ctx, model.StatusRunning, model.StatusAwaiting)
	if err != nil {
		return err
	}
	for _, m := range metas {
		e.dispatch(m.RunID)
	}
	return nil
}

// dispatch 将 RunID 投递给执行路径：同步模式下直接 run，否则进入调度器队列。
func (e *Engine) dispatch(runID model.RunID) {
	if e.testOpts.runSync {
		e.run(e.ctx, runID)
		return
	}
	e.sched.enqueue(runID)
}

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
