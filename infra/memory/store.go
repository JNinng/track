package memory

import (
	"context"
	"sync"
	"time"

	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// logMetaStore 同时实现 store.Reader / store.Writer / store.Meta。
type logMetaStore struct {
	mu    sync.Mutex
	logs  map[model.RunID][]model.LogEntry
	metas map[model.RunID]*model.RunMeta
	now   func() time.Time
}

func newLogMetaStore() *logMetaStore {
	return &logMetaStore{
		logs:  make(map[model.RunID][]model.LogEntry),
		metas: make(map[model.RunID]*model.RunMeta),
		now:   time.Now,
	}
}

// Append 实现 store.Writer。
func (s *logMetaStore) Append(_ context.Context, runID model.RunID, entry model.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := model.LogEntry{
		Kind:    entry.Kind,
		Label:   entry.Label,
		Err:     entry.Err,
		Payload: append([]byte(nil), entry.Payload...),
	}
	s.logs[runID] = append(s.logs[runID], cp)
	return nil
}

// Read 实现 store.Reader，按追加顺序返回日志的副本。
func (s *logMetaStore) Read(_ context.Context, runID model.RunID) ([]model.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.logs[runID]
	out := make([]model.LogEntry, len(src))
	for i, e := range src {
		out[i] = e
		out[i].Payload = append([]byte(nil), e.Payload...)
	}
	return out, nil
}

// UpdateStatus 实现 store.Meta，不存在则创建。
func (s *logMetaStore) UpdateStatus(_ context.Context, runID model.RunID, status model.RunStatus, opts ...store.MetaOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.metas[runID]
	if !ok {
		m = &model.RunMeta{RunID: runID, CreatedAt: s.now()}
		s.metas[runID] = m
	}
	m.Status = status
	for _, o := range opts {
		o(m)
	}
	m.UpdatedAt = s.now()
	return nil
}

// GetResult 实现 store.Meta。
func (s *logMetaStore) GetResult(_ context.Context, runID model.RunID) (*model.RunMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.metas[runID]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

// ListByStatus 实现 store.Meta。
func (s *logMetaStore) ListByStatus(_ context.Context, statuses ...model.RunStatus) ([]model.RunMeta, error) {
	want := make(map[model.RunStatus]struct{}, len(statuses))
	for _, st := range statuses {
		want[st] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.RunMeta, 0)
	for _, m := range s.metas {
		if _, ok := want[m.Status]; ok {
			out = append(out, *m)
		}
	}
	return out, nil
}

// SetNow 注入自定义当前时间函数，便于测试控制时间戳。
func (s *logMetaStore) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// Store 是一个满足 store.Interface 的内存后端，组合了日志/元数据、锁与信箱。
//
// 使用 memory.New() 构造，传入 engine.NewEngine。
type Store struct {
	*logMetaStore
	*Locker
	*Mailbox
}

// New 创建一个完整的内存后端，满足 store.Interface。
func New() *Store {
	return &Store{
		logMetaStore: newLogMetaStore(),
		Locker:       NewLocker(),
		Mailbox:      NewMailbox(),
	}
}

// 编译期保证 Store 实现 store.Interface。
var _ store.Interface = (*Store)(nil)
