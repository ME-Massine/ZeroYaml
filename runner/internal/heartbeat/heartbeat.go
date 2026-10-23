// Package heartbeat drives the Runner's periodic liveness reporting to the
// Control Plane. It owns only the interval loop; the Control Plane owns what
// a missed heartbeat means, so a failed or declined heartbeat here is logged
// and never stops the loop or the Runner process.
package heartbeat

import (
	"context"
	"log/slog"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/heartbeatclient"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
)

// Ticker is the seam Run uses to schedule attempts, so tests can drive the
// loop without waiting on real wall-clock intervals.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time { return t.ticker.C }
func (t *realTicker) Stop()               { t.ticker.Stop() }

func newRealTicker(interval time.Duration) Ticker {
	return &realTicker{ticker: time.NewTicker(interval)}
}

type sendFunc func(ctx context.Context, address string, identity runneridentity.Identity) (heartbeatclient.Result, error)

// Run sends a heartbeat for identity to the Control Plane at address every
// interval, each attempt bounded by timeout, until ctx is done. It returns
// only when ctx is cancelled, so callers run it in its own goroutine.
func Run(
	ctx context.Context,
	address string,
	identity runneridentity.Identity,
	interval time.Duration,
	timeout time.Duration,
	logger *slog.Logger,
) {
	run(ctx, address, identity, interval, timeout, logger, heartbeatclient.Heartbeat, newRealTicker)
}

func run(
	ctx context.Context,
	address string,
	identity runneridentity.Identity,
	interval time.Duration,
	timeout time.Duration,
	logger *slog.Logger,
	send sendFunc,
	newTicker func(time.Duration) Ticker,
) {
	ticker := newTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			sendOnce(ctx, address, identity, timeout, logger, send)
		}
	}
}

func sendOnce(
	ctx context.Context,
	address string,
	identity runneridentity.Identity,
	timeout time.Duration,
	logger *slog.Logger,
	send sendFunc,
) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := send(callCtx, address, identity)
	if err != nil {
		logger.Warn(
			"runner heartbeat failed",
			"runner_id", identity.RunnerID,
			"instance_id", identity.InstanceID,
			"error", err,
		)
		return
	}

	if result.Decision == heartbeatclient.DecisionUnknownRunner {
		logger.Warn(
			"runner heartbeat reported unknown runner",
			"runner_id", identity.RunnerID,
			"instance_id", identity.InstanceID,
			"message", result.Message,
		)
	}
}
