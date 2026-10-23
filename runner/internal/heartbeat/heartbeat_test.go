package heartbeat

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/heartbeatclient"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
)

const testTimeout = 5 * time.Second

func TestRunSendsOneHeartbeatPerTick(t *testing.T) {
	ticker := newFakeTicker()
	calls := make(chan struct{}, 8)
	send := func(ctx context.Context, address string, identity runneridentity.Identity) (heartbeatclient.Result, error) {
		calls <- struct{}{}
		return heartbeatclient.Result{Decision: heartbeatclient.DecisionAcknowledged}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		run(ctx, "address", newTestIdentity(t), time.Second, time.Second, silentLogger(), send, ticker.factory())
		close(done)
	}()

	ticker.tick()
	waitForCall(t, calls)

	ticker.tick()
	waitForCall(t, calls)

	cancel()
	waitForDone(t, done)

	if !ticker.wasStopped() {
		t.Error("expected the ticker to be stopped when the loop exits")
	}
}

func TestRunContinuesAfterASendError(t *testing.T) {
	ticker := newFakeTicker()
	calls := make(chan struct{}, 8)
	var attempt atomic.Int32
	send := func(ctx context.Context, address string, identity runneridentity.Identity) (heartbeatclient.Result, error) {
		calls <- struct{}{}
		if attempt.Add(1) == 1 {
			return heartbeatclient.Result{}, errors.New("unreachable control plane")
		}
		return heartbeatclient.Result{Decision: heartbeatclient.DecisionAcknowledged}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		run(ctx, "address", newTestIdentity(t), time.Second, time.Second, silentLogger(), send, ticker.factory())
		close(done)
	}()
	defer func() {
		cancel()
		waitForDone(t, done)
	}()

	ticker.tick()
	waitForCall(t, calls)

	ticker.tick()
	waitForCall(t, calls)

	if got := attempt.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2 (a failed heartbeat must not stop the loop)", got)
	}
}

func TestRunContinuesAfterAnUnknownRunnerDecision(t *testing.T) {
	ticker := newFakeTicker()
	calls := make(chan struct{}, 8)
	send := func(ctx context.Context, address string, identity runneridentity.Identity) (heartbeatclient.Result, error) {
		calls <- struct{}{}
		return heartbeatclient.Result{Decision: heartbeatclient.DecisionUnknownRunner}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		run(ctx, "address", newTestIdentity(t), time.Second, time.Second, silentLogger(), send, ticker.factory())
		close(done)
	}()

	ticker.tick()
	waitForCall(t, calls)
	ticker.tick()
	waitForCall(t, calls)

	cancel()
	waitForDone(t, done)
}

func TestRunSendsNoHeartbeatBeforeTheFirstTick(t *testing.T) {
	ticker := newFakeTicker()
	calls := make(chan struct{}, 8)
	send := func(ctx context.Context, address string, identity runneridentity.Identity) (heartbeatclient.Result, error) {
		calls <- struct{}{}
		return heartbeatclient.Result{Decision: heartbeatclient.DecisionAcknowledged}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		run(ctx, "address", newTestIdentity(t), time.Second, time.Second, silentLogger(), send, ticker.factory())
		close(done)
	}()

	select {
	case <-calls:
		t.Fatal("expected no heartbeat before the first tick")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	waitForDone(t, done)
}

// fakeTicker gives the test manual control over when Run's loop wakes up,
// instead of waiting on a real interval.
type fakeTicker struct {
	ch      chan time.Time
	stopped chan struct{}
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time), stopped: make(chan struct{})}
}

func (f *fakeTicker) factory() func(time.Duration) Ticker {
	return func(time.Duration) Ticker { return f }
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }

func (f *fakeTicker) Stop() {
	close(f.stopped)
}

func (f *fakeTicker) tick() {
	f.ch <- time.Now()
}

func (f *fakeTicker) wasStopped() bool {
	select {
	case <-f.stopped:
		return true
	default:
		return false
	}
}

func waitForCall(t *testing.T, calls <-chan struct{}) {
	t.Helper()

	select {
	case <-calls:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a heartbeat send attempt")
	}
}

func waitForDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the heartbeat loop to stop")
	}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestIdentity(t *testing.T) runneridentity.Identity {
	t.Helper()

	identity, err := runneridentity.New(
		"runner-dev-01",
		"0.1.0",
		runneridentity.ProtocolVersion,
		runneridentity.DefaultCapabilities(),
	)
	if err != nil {
		t.Fatalf("runneridentity.New() returned an error: %v", err)
	}

	return identity
}
