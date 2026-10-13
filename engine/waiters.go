package engine

import (
	"sync"

	"github.com/jninng/track/model"
)

// waiterRegistry 是 WaitDone 的进程内等待者注册表（设计文档 5.1 节「WaitDone
// 的等待契约」）。
//
// runID -> 等待者 channel 列表。纯观测旁路：不持久化、不参与重放与调度决策，
// 引擎从不依据登记存在与否做任何状态写入。
//
// 不变量：notify 必须在锁内快照并删除登记、在锁外逐个 close——锁内 close 会
// 让被唤醒方醒来后立即与注册表争抢同一把锁。close 至多发生一次：登记已随
// notify 删除，重复调用拿到空列表；登记到关闭之间退出的等待者已由 remove
// 按身份摘除，不会被误关。
type waiterRegistry struct {
	mu   sync.Mutex
	byID map[model.RunID][]chan struct{}
}

func newWaiterRegistry() *waiterRegistry {
	return &waiterRegistry{byID: make(map[model.RunID][]chan struct{})}
}

// add 在 runID 的等待者列表尾部登记 ch。
func (r *waiterRegistry) add(runID model.RunID, ch chan struct{}) {
	r.mu.Lock()
	r.byID[runID] = append(r.byID[runID], ch)
	r.mu.Unlock()
}

// remove 按身份摘除一次登记（WaitDone 因 ctx 退出返回后清理，防泄漏）。
// 列表摘空后删除 key，避免注册表随 RunID 无界增长。
func (r *waiterRegistry) remove(runID model.RunID, ch chan struct{}) {
	r.mu.Lock()
	list := r.byID[runID]
	for i, c := range list {
		if c == ch {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(r.byID, runID)
	} else {
		r.byID[runID] = list
	}
	r.mu.Unlock()
}

// notify 关闭并清除 runID 的全部等待者 channel，唤醒 WaitDone 调用方。
//
// 必须在终态成功落盘之后调用（succeed/fail/Cancel 三处终态转换点均如此），
// 保证唤醒方重新 GetResult 读到的必为终态元数据。同一 RunID 的终态写入被
// per-run 锁串行化，不会并发触发本方法。
func (r *waiterRegistry) notify(runID model.RunID) {
	r.mu.Lock()
	list := r.byID[runID]
	delete(r.byID, runID)
	r.mu.Unlock()
	for _, ch := range list {
		close(ch)
	}
}

// len 返回当前有登记的 RunID 数量（测试断言用）。
func (r *waiterRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}

// count 返回 runID 当前登记的等待者数量（测试断言用）。
func (r *waiterRegistry) count(runID model.RunID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID[runID])
}
