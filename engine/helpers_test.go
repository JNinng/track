package engine

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/jninng/track/infra/memory"
	"github.com/jninng/track/model"
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

// logsEqual 比较两份日志的 StepID 顺序与 Payload 内容是否完全一致。
//
// 这是确定性重放的核心断言（设计文档 12.1）：给定相同输入与历史，
// 重放生成的日志必须与首次执行完全匹配。
func logsEqual(a, b []model.LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].StepID != b[i].StepID {
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
