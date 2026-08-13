package clock

import (
	"testing"
	"time"
)

func TestFakeClockNow(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewFakeClockAt(start)
	if !c.Now().Equal(start) {
		t.Fatalf("Now() = %v, want %v", c.Now(), start)
	}
}

func TestFakeClockAdvance(t *testing.T) {
	c := NewFakeClockAt(time.Unix(0, 0).UTC())
	c.Advance(10 * time.Second)
	if got := c.Now().Unix(); got != 10 {
		t.Fatalf("After advance, Now().Unix() = %d, want 10", got)
	}
}

func TestFakeClockAfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock()
	ch := c.After(5 * time.Second)
	// 未到期前不应触发。
	select {
	case <-ch:
		t.Fatal("timer fired before deadline")
	default:
	}
	if !c.HasPendingTimers() {
		t.Fatal("expected pending timer before advance")
	}
	c.Advance(5 * time.Second)
	select {
	case got := <-ch:
		if want := c.Now(); !got.Equal(want) {
			t.Fatalf("received time = %v, want %v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timer did not fire after advance past deadline")
	}
	if c.HasPendingTimers() {
		t.Fatal("expected no pending timers after fire")
	}
}

func TestFakeClockAfterPartialAdvanceDoesNotFire(t *testing.T) {
	c := NewFakeClock()
	ch := c.After(10 * time.Second)
	c.Advance(3 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired too early")
	default:
	}
	c.Advance(7 * time.Second)
	select {
	case <-ch:
	default:
		t.Fatal("timer did not fire after reaching deadline")
	}
}

func TestFakeClockAfterZeroFiresImmediately(t *testing.T) {
	c := NewFakeClock()
	ch := c.After(0)
	select {
	case <-ch:
	default:
		t.Fatal("zero-duration timer should fire on registration")
	}
}

// 立即触发（d<=0）的定时器不得残留在内部 timers 列表中：
// 否则 HasPendingTimers 误报 true（语义上定时器已触发、不再 pending），
// 且 Advance 永远无法清除它，多次调用将导致切片无界增长。
func TestFakeClockAfterZeroDoesNotLeak(t *testing.T) {
	c := NewFakeClock()
	c.After(0)
	c.After(-1) // 负值同样立即触发

	if c.HasPendingTimers() {
		t.Fatal("HasPendingTimers=true after After(0)/After(-1); already-fired timers must not be pending")
	}
	// Advance 也不应把它当作待触发条目残留。
	c.Advance(5 * time.Second)
	if c.HasPendingTimers() {
		t.Fatal("already-fired zero-duration timer leaked across Advance")
	}
}
