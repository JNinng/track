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

// setNow 注入自定义当前时间函数，便于测试控制 meta 时间戳。
//
// 刻意未导出：Store 同时嵌入 *Locker（也有 SetNow），若二者同名会导致
// Store.SetNow 选择器歧义。保留内部注入点、把公开的 SetNow 留给 Locker
// （租约时钟是更需要外部控制的语义）。
func (s *logMetaStore) setNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// Store 是一个满足 store.Interface 的内存后端，组合了日志/元数据、锁与信箱。
//
// 使用 memory.New(opts...) 构造，传入 engine.NewEngine。
type Store struct {
	*logMetaStore
	*Locker
	*Mailbox
}

// Option 配置内存后端的可选参数，遵循仓库的 option-function 模式。
type Option func(*Store)

// WithLease 设置 Locker 的租约时长（缺省 30s）。
//
// 用于「单次 run() 同步段耗时较长」的场景：把租约调到大于最坏同步执行总时长，
// 避免租约在中途过期、被其它 Worker 接管。注意引擎不会自动续期——租约需由
// 调用方依据业务步骤断定（详见 docs/engine.md §4.2）。
func WithLease(d time.Duration) Option {
	return func(s *Store) { s.Locker = NewLockerWithLease(d) }
}

// New 创建一个完整的内存后端，满足 store.Interface。opts 可定制锁租约等参数。
func New(opts ...Option) *Store {
	s := &Store{
		logMetaStore: newLogMetaStore(),
		Locker:       NewLocker(),
		Mailbox:      NewMailbox(),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// 编译期保证 Store 实现 store.Interface。
var _ store.Interface = (*Store)(nil)
