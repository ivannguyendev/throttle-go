package breaker

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestBreakerOpensAfterThresholdConsecutiveFailures(t *testing.T) {
	b := New(3, time.Second)
	for i := range 2 {
		if b.RecordFailure() {
			t.Fatalf("failure %d opened the breaker early", i+1)
		}
	}
	if !b.RecordFailure() {
		t.Fatal("third failure did not open the breaker")
	}
	if got := b.Admit(); got != Open {
		t.Fatalf("Admit() = %v, want Open", got)
	}
}

func TestBreakerSuccessResetsTheFailureCount(t *testing.T) {
	b := New(2, time.Second)
	b.RecordFailure()
	b.RecordSuccess()
	if b.RecordFailure() {
		t.Fatal("failure count was not reset by a success")
	}
	if b.IsOpen() {
		t.Fatal("breaker open after one failure")
	}
}

func TestBreakerAdmitsOneProbeAfterCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(1, 10*time.Second)
		b.RecordFailure()
		time.Sleep(9 * time.Second)
		if got := b.Admit(); got != Open {
			t.Fatalf("Admit() during cooldown = %v, want Open", got)
		}
		time.Sleep(time.Second)
		if got := b.Admit(); got != Probe {
			t.Fatalf("Admit() after cooldown = %v, want Probe", got)
		}
		if got := b.Admit(); got != Open {
			t.Fatalf("second Admit() while probing = %v, want Open", got)
		}
	})
}

func TestBreakerProbeOutcomes(t *testing.T) {
	tests := []struct {
		name     string
		record   func(*Breaker)
		wantOpen bool
		wantNext Admission
	}{
		{"a successful probe closes the breaker", (*Breaker).RecordSuccess, false, Closed},
		{"a failed probe reopens it for another cooldown", func(b *Breaker) { b.RecordFailure() }, true, Open},
		{"an ignored probe lets the next call probe again", (*Breaker).RecordIgnored, true, Probe},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := New(1, time.Second)
				b.RecordFailure()
				time.Sleep(time.Second)
				if got := b.Admit(); got != Probe {
					t.Fatalf("Admit() = %v, want Probe", got)
				}
				tt.record(b)
				if b.IsOpen() != tt.wantOpen {
					t.Fatalf("IsOpen() = %v, want %v", b.IsOpen(), tt.wantOpen)
				}
				if got := b.Admit(); got != tt.wantNext {
					t.Fatalf("next Admit() = %v, want %v", got, tt.wantNext)
				}
			})
		})
	}
}

func TestBreakerThresholdBelowOneIsTreatedAsOne(t *testing.T) {
	if !New(0, time.Second).RecordFailure() {
		t.Fatal("threshold 0 did not open on the first failure")
	}
}
