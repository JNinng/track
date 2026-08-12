package memory

import (
	"context"
	"sync"

	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// mailboxKey 唯一标识一个 (RunID, Signal) 组合。
type mailboxKey struct {
	run model.RunID
	sig model.Signal
}

// Mailbox 是 store.Mailbox 的内存实现。
type Mailbox struct {
	mu      sync.Mutex
	signals map[mailboxKey][]byte
	pushes  map[mailboxKey]int // 调试计数：每个键被 Push 的次数
}

// NewMailbox 创建一个空的 Mailbox。
func NewMailbox() *Mailbox {
	return &Mailbox{
		signals: make(map[mailboxKey][]byte),
		pushes:  make(map[mailboxKey]int),
	}
}

// Push 持久化存储（覆盖）一个信号。
func (m *Mailbox) Push(_ context.Context, runID model.RunID, signal model.Signal, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := mailboxKey{runID, signal}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.signals[k] = cp
	m.pushes[k]++
	return nil
}

// Fetch 获取信号但不删除。不存在时返回 store.ErrNotFound。
func (m *Mailbox) Fetch(_ context.Context, runID model.RunID, signal model.Signal) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.signals[mailboxKey{runID, signal}]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

// Ack 删除已被消费的信号。
func (m *Mailbox) Ack(_ context.Context, runID model.RunID, signal model.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.signals, mailboxKey{runID, signal})
	return nil
}

// PushCount 返回某 (RunID, Signal) 被 Push 的次数，用于测试断言。
func (m *Mailbox) PushCount(runID model.RunID, signal model.Signal) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pushes[mailboxKey{runID, signal}]
}

// Has 报告是否存在未消费的信号，用于测试断言。
func (m *Mailbox) Has(runID model.RunID, signal model.Signal) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.signals[mailboxKey{runID, signal}]
	return ok
}
