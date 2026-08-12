package policy

import (
	"errors"
	"testing"
	"time"
)

var errFail = errors.New("fail")

func TestNoRetry(t *testing.T) {
	p := NoRetry{}
	if _, ok := p.Next(errFail); ok {
		t.Fatal("NoRetry should not retry")
	}
}

func TestFixedDelayRetry(t *testing.T) {
	p := NewFixedDelay(10*time.Millisecond, 3)
	// 3 次执行 → 2 次重试。
	if d, ok := p.Next(errFail); !ok || d != 10*time.Millisecond {
		t.Fatalf("retry 1: got (%v,%v), want (10ms,true)", d, ok)
	}
	if d, ok := p.Next(errFail); !ok || d != 10*time.Millisecond {
		t.Fatalf("retry 2: got (%v,%v), want (10ms,true)", d, ok)
	}
	if _, ok := p.Next(errFail); ok {
		t.Fatal("should stop after MaxAttempts")
	}
}

func TestFixedDelayCloneIndependent(t *testing.T) {
	p := NewFixedDelay(10*time.Millisecond, 3)
	clone := p.Clone()
	p.Next(errFail)
	// 克隆副本应保持独立计数。
	c2 := clone.(Cloner).Clone().(*FixedDelay)
	if c2.attempts != 0 {
		t.Fatalf("clone state leaked from original: attempts=%d", c2.attempts)
	}
}

func TestExponentialBackoff(t *testing.T) {
	p := NewExponentialBackoff(10*time.Millisecond, 2, 100*time.Millisecond, 4)
	// k=1: 10ms, k=2: 20ms, k=3: 40ms, 第 4 次执行后停止。
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		d, ok := p.Next(errFail)
		if !ok || d != w {
			t.Fatalf("retry %d: got (%v,%v), want (%v,true)", i+1, d, ok, w)
		}
	}
	if _, ok := p.Next(errFail); ok {
		t.Fatal("should stop after MaxAttempts")
	}
}

func TestExponentialBackoffCap(t *testing.T) {
	p := NewExponentialBackoff(10*time.Millisecond, 10, 50*time.Millisecond, 5)
	// 10*10=100 -> 封顶到 50ms。
	d, ok := p.Next(errFail) // 10ms
	if !ok || d != 10*time.Millisecond {
		t.Fatalf("first: got (%v,%v)", d, ok)
	}
	d, ok = p.Next(errFail) // 100 -> capped 50
	if !ok || d != 50*time.Millisecond {
		t.Fatalf("capped: got (%v,%v), want 50ms", d, ok)
	}
}

func TestForExecNil(t *testing.T) {
	if _, ok := ForExec(nil).(NoRetry); !ok {
		t.Fatal("ForExec(nil) should return NoRetry")
	}
}
