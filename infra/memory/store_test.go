package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jninng/track/model"
	"github.com/jninng/track/store"
)

func ctxn() context.Context { return context.Background() }

func TestStoreLogsAppendAndRead(t *testing.T) {
	s := New()
	rid := model.RunID("r1")
	e1 := model.LogEntry{StepID: "s1", Payload: []byte(`1`)}
	e2 := model.LogEntry{StepID: "s2", Payload: []byte(`2`)}
	if err := s.Append(ctxn(), rid, e1); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctxn(), rid, e2); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(ctxn(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].StepID != "s1" || got[1].StepID != "s2" {
		t.Fatalf("unexpected logs: %+v", got)
	}
	// 副本隔离：修改返回值不影响内部状态。
	got[0].Payload[0] = '9'
	again, _ := s.Read(ctxn(), rid)
	if again[0].Payload[0] != '1' {
		t.Fatal("Read returned a reference to internal storage")
	}
}

func TestStoreMetaUpsertAndGet(t *testing.T) {
	s := New()
	rid := model.RunID("r1")
	if err := s.UpdateStatus(ctxn(), rid, model.StatusRunning, store.WithName("W"), store.WithVersion("v1")); err != nil {
		t.Fatal(err)
	}
	m, err := s.GetResult(ctxn(), rid)
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "W" || m.Version != "v1" || m.Status != model.StatusRunning {
		t.Fatalf("unexpected meta: %+v", m)
	}
	if err := s.UpdateStatus(ctxn(), rid, model.StatusSucceeded, store.WithOutput([]byte("ok"))); err != nil {
		t.Fatal(err)
	}
	m, _ = s.GetResult(ctxn(), rid)
	if m.Status != model.StatusSucceeded || string(m.Output) != "ok" {
		t.Fatalf("unexpected updated meta: %+v", m)
	}
}

func TestStoreGetResultNotFound(t *testing.T) {
	s := New()
	_, err := s.GetResult(ctxn(), model.RunID("missing"))
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestStoreListByStatus(t *testing.T) {
	s := New()
	s.UpdateStatus(ctxn(), "a", model.StatusRunning)
	s.UpdateStatus(ctxn(), "b", model.StatusAwaiting)
	s.UpdateStatus(ctxn(), "c", model.StatusSucceeded)
	got, err := s.ListByStatus(ctxn(), model.StatusRunning, model.StatusAwaiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
}

func TestLockerMutualExclusion(t *testing.T) {
	l := NewLocker()
	rid := model.RunID("r1")
	ok1, err := l.Acquire(ctxn(), rid)
	if err != nil || !ok1 {
		t.Fatal("first acquire should succeed")
	}
	ok2, _ := l.Acquire(ctxn(), rid)
	if ok2 {
		t.Fatal("second acquire should fail while held")
	}
	if !l.IsLocked(rid) {
		t.Fatal("should report locked")
	}
	l.Release(ctxn(), rid)
	if l.IsLocked(rid) {
		t.Fatal("should report unlocked after release")
	}
	ok3, _ := l.Acquire(ctxn(), rid)
	if !ok3 {
		t.Fatal("acquire after release should succeed")
	}
}

func TestLockerLeaseExpiry(t *testing.T) {
	l := NewLockerWithLease(time.Second)
	now := time.Now()
	l.SetNow(func() time.Time { return now })
	rid := model.RunID("r1")
	if ok, _ := l.Acquire(ctxn(), rid); !ok {
		t.Fatal("acquire failed")
	}
	// 时间未到，仍持有。
	l.SetNow(func() time.Time { return now.Add(500 * time.Millisecond) })
	if ok, _ := l.Acquire(ctxn(), rid); ok {
		t.Fatal("should still be locked before lease expiry")
	}
	// 越过租约，应可被他人获取。
	l.SetNow(func() time.Time { return now.Add(2 * time.Second) })
	if ok, _ := l.Acquire(ctxn(), rid); !ok {
		t.Fatal("should acquire after lease expiry")
	}
}

func TestLockerExpire(t *testing.T) {
	l := NewLocker()
	rid := model.RunID("r1")
	l.Acquire(ctxn(), rid)
	l.Expire(rid)
	if ok, _ := l.Acquire(ctxn(), rid); !ok {
		t.Fatal("should acquire after Expire")
	}
}

func TestMailboxPushFetchAck(t *testing.T) {
	mb := NewMailbox()
	rid := model.RunID("r1")
	sig := model.Signal("ready")
	if _, err := mb.Fetch(ctxn(), rid, sig); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("fetch before push should be ErrNotFound")
	}
	if err := mb.Push(ctxn(), rid, sig, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if mb.PushCount(rid, sig) != 1 {
		t.Fatal("push count mismatch")
	}
	got, err := mb.Fetch(ctxn(), rid, sig)
	if err != nil || string(got) != "payload" {
		t.Fatalf("fetch after push: %v %s", err, got)
	}
	if !mb.Has(rid, sig) {
		t.Fatal("Has should be true before Ack")
	}
	if err := mb.Ack(ctxn(), rid, sig); err != nil {
		t.Fatal(err)
	}
	if mb.Has(rid, sig) {
		t.Fatal("Has should be false after Ack")
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	s := New()
	rid := model.RunID("r1")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Append(ctxn(), rid, model.LogEntry{StepID: model.StepID("a"), Payload: []byte{byte(i)}})
			s.Read(ctxn(), rid)
		}(i)
	}
	wg.Wait()
	logs, _ := s.Read(ctxn(), rid)
	if len(logs) != 50 {
		t.Fatalf("want 50 logs, got %d", len(logs))
	}
}
