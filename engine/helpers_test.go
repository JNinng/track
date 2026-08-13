package engine

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

// jsonUnmarshal 是 encoding/json.Unmarshal 的简短别名，便于测试阅读。
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// itoa 是 strconv.Itoa 的简短别名。
func itoa(n int) string { return strconv.Itoa(n) }

// mustReadLogs 读取并返回指定实例的全部日志，失败时终止测试。
func mustReadLogs(t *testing.T, s *memory.Store, runID model.RunID) []model.LogEntry {
	t.Helper()
	logs, err := s.Read(context.Background(), runID)
	if err != nil {
		t.Fatalf("Read logs: %v", err)
	}
	return logs
}

// logsEqual 比较两份日志的 Kind/Label 顺序与 Payload 内容是否完全一致。
//
// 这是确定性重放的核心断言（设计文档 12.1）：给定相同输入与历史，
// 重放生成的日志必须与首次执行完全匹配。运行时 consume 只按位置 + Kind 匹配，
// 此处额外比较 Label 仅为加严测试（同码黄金样本下必然一致）。
func logsEqual(a, b []model.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind {
			return false
		}
		if a[i].Label != b[i].Label {
			return false
		}
		if string(a[i].Payload) != string(b[i].Payload) {
			return false
		}
		if a[i].Err != b[i].Err {
			return false
		}
	}
	return true
}

// waitFor 使用 FakeClock 的时间推进 + 短暂轮询等待终态，最长真实等待 d。
func waitFor(t *testing.T, e *Engine, runID model.RunID, d time.Duration) *model.RunMeta {
	t.Helper()
	return awaitStatus(t, e, runID, d)
}

// fetchCountingStore 包装一个 store.Interface，统计 Fetch 调用次数，用于验证
// Await 原语不会对同一信号重复 Fetch。
type fetchCountingStore struct {
	store.Interface
	fetches int32
}

func (f *fetchCountingStore) Fetch(ctx context.Context, runID model.RunID, signal model.Signal) ([]byte, error) {
	atomic.AddInt32(&f.fetches, 1)
	return f.Interface.Fetch(ctx, runID, signal)
}

// ackFailingStore 包装一个 store.Interface，其 Ack 恒定返回指定错误，用于验证
// Ack 失败不会被判定为工作流失败（决策已落盘，日志为确定性真相）。
type ackFailingStore struct {
	store.Interface
	ackErr error
}

func (a *ackFailingStore) Ack(ctx context.Context, runID model.RunID, signal model.Signal) error {
	if a.ackErr != nil {
		return a.ackErr
	}
	return a.Interface.Ack(ctx, runID, signal)
}
