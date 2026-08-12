package model

import "testing"

func TestRunStatusString(t *testing.T) {
	cases := []struct {
		status RunStatus
		want   string
	}{
		{StatusRunning, "Running"},
		{StatusCancelled, "Cancelled"},
		{StatusSucceeded, "Succeeded"},
		{StatusFailed, "Failed"},
		{StatusAwaiting, "Awaiting"},
		{RunStatus(99), "Unknown"},
	}
	for _, c := range cases {
		if got := c.status.String(); got != c.want {
			t.Errorf("(%d).String() = %q, want %q", c.status, got, c.want)
		}
	}
}

func TestRunStatusIsTerminal(t *testing.T) {
	terminal := []RunStatus{StatusSucceeded, StatusFailed, StatusCancelled}
	nonTerminal := []RunStatus{StatusRunning, StatusAwaiting}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}
