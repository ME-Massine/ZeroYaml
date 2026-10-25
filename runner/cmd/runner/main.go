package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/grpcserver"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/heartbeat"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/jobexecution"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/jobstatus"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/registrationclient"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runnerconfig"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/sandbox"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/serverlifecycle"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/startupregistration"
	"google.golang.org/grpc"
)

// dockerBinary is resolved through PATH, as for any Docker CLI user.
const dockerBinary = "docker"

// dockerProbeTimeout bounds the startup check of the Docker daemon so that a
// hung daemon delays startup by at most this long.
const dockerProbeTimeout = 5 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("ZeroYAML Runner stopped with an error: %v", err)
	}
}

// run wires the Runner process and serves until a termination signal arrives.
// Keeping the wiring here leaves main free of error handling and makes the
// startup and shutdown order explicit.
func run() error {
	cfg, err := runnerconfig.Load()
	if err != nil {
		return fmt.Errorf("invalid runner configuration: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	identity, err := runneridentity.New(
		cfg.RunnerID,
		cfg.Version,
		runneridentity.ProtocolVersion,
		probeDockerCapabilities(logger),
	)
	if err != nil {
		return fmt.Errorf("failed to create runner identity: %w", err)
	}

	logReporter, err := joblogs.NewReporter(cfg.JobLogs, identity, cfg.StatusReportTimeout, logger)
	if err != nil {
		return fmt.Errorf("invalid job log configuration: %w", err)
	}
	var logSink joblogs.Sink
	if logReporter != nil {
		logSink = logReporter
		// Registered before dispatcher.Close so executions drain first.
		defer func() {
			if err := logReporter.Close(); err != nil {
				logger.Warn("job log connection close failed")
			}
		}()
	} else {
		logger.Info("job log delivery disabled; configure ZEROYAML_JOB_LOGS_ADDRESS and TLS files to enable")
	}

	// SIGINT covers local development and SIGTERM covers container and service
	// managers. The signal handler is installed before the listener is opened so
	// that a signal arriving during startup still triggers an ordered shutdown.
	ctx, stopListeningForSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopListeningForSignals()

	// Registration happens once, before the identity is handed to the gRPC
	// server, so GetInfo and Ping always report a status consistent with what
	// the Control Plane was told. A Control Plane that is unreachable or that
	// rejects the request leaves the Runner unavailable rather than failing
	// startup, because Ping and GetInfo must stay reachable for diagnosis.
	identity = registerWithControlPlane(ctx, cfg, identity)

	// A Runner accepts Jobs only when the Control Plane accepted it and its
	// Docker daemon answered at startup; otherwise every dispatch is rejected as
	// unavailable instead of failing later inside the sandbox.
	identity.AcceptingWork = identity.Status == runneridentity.StatusReady && identity.Capabilities.DockerAvailable

	// The heartbeat loop only starts once registration reported the Runner
	// ready; a Runner the Control Plane never accepted has nothing to keep
	// alive on the registry side. It stops on its own once ctx is cancelled.
	if identity.Status == runneridentity.StatusReady {
		go heartbeat.Run(ctx, cfg.ControlPlaneAddress, identity, cfg.HeartbeatInterval, cfg.HeartbeatTimeout, logger)
	}

	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", cfg.GRPCAddress, err)
	}

	// Executions run under ctx, so a termination signal stops running
	// containers. The deferred Close cancels ctx first, which also covers a
	// server failure without a signal, and then waits for every execution's
	// cleanup and final report.
	//
	// The RPC drain and the execution drain share one budget that starts when
	// shutdown is requested, so the process terminates within ShutdownTimeout of
	// the signal rather than within the RPC drain plus an unbounded wait on
	// container cleanup and status reporting.
	dispatcher := jobexecution.NewDispatcher(
		ctx,
		sandbox.New(dockerBinary, logger, logSink),
		jobstatus.NewReporter(cfg.ControlPlaneAddress, identity, cfg.StatusReportTimeout, logger),
		sandbox.Settings{
			Image:           cfg.JobImage,
			CheckoutImage:   cfg.CheckoutImage,
			Timeout:         cfg.JobTimeout,
			LocalSourceRoot: cfg.LocalSourceRoot,
			Limits:          sandbox.DefaultLimits(),
		},
		cfg.MaxConcurrentJobs,
		logger,
	)
	shutdownBudget, endShutdownBudget := budgetAfterCancel(ctx, cfg.ShutdownTimeout)
	defer endShutdownBudget()

	defer func() {
		stopListeningForSignals()

		if err := dispatcher.Close(shutdownBudget); err != nil {
			logger.Warn(
				"runner exited before every job execution finished",
				slog.String("error", err.Error()),
				slog.Duration("shutdown_timeout", cfg.ShutdownTimeout),
			)
		}
	}()

	server := grpc.NewServer()

	runnerv1.RegisterRunnerServiceServer(
		server,
		grpcserver.New(identity, dispatcher, logger),
	)

	log.Printf(
		"ZeroYAML Runner gRPC server listening on %s - runner %s - instance %s - version %s - protocol %s",
		cfg.GRPCAddress,
		cfg.RunnerID,
		identity.InstanceID,
		cfg.Version,
		identity.ProtocolVersion,
	)

	// ServeUntilShutdown owns the listener from this point and closes it on every
	// stop path.
	if err := serverlifecycle.ServeUntilShutdown(ctx, server, listener, cfg.ShutdownTimeout); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	log.Printf(
		"ZeroYAML Runner gRPC server shut down - runner %s - drain timeout %s",
		cfg.RunnerID,
		cfg.ShutdownTimeout,
	)

	return nil
}

// budgetAfterCancel returns a context that stays alive until timeout has
// elapsed since ctx was cancelled, and a function that ends it early.
//
// It turns the shutdown timeout into one deadline for the whole shutdown
// sequence: the clock starts when shutdown is requested rather than when the
// previous stage finished, so the stages cannot add up past the configured
// budget. The returned context is detached from ctx, because it must stay
// usable exactly when ctx is already cancelled.
func budgetAfterCancel(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	budget, endBudget := context.WithCancel(context.WithoutCancel(ctx))

	go func() {
		select {
		case <-ctx.Done():
		case <-budget.Done():
			return
		}

		expiry := time.NewTimer(timeout)
		defer expiry.Stop()

		select {
		case <-expiry.C:
			endBudget()
		case <-budget.Done():
		}
	}()

	return budget, endBudget
}

// probeDockerCapabilities reports Docker as an available executor only when
// the daemon answers within dockerProbeTimeout. An unreachable daemon is logged
// and leaves the Runner serving without execution support, so Ping and GetInfo
// stay reachable for diagnosis.
func probeDockerCapabilities(logger *slog.Logger) runneridentity.Capabilities {
	capabilities := runneridentity.DefaultCapabilities()

	version, err := sandbox.Probe(context.Background(), dockerBinary, dockerProbeTimeout)
	if err != nil {
		logger.Warn("docker daemon unavailable - runner will not accept jobs", slog.String("error", err.Error()))

		return capabilities
	}

	logger.Info("docker daemon available", slog.String("docker_version", version))
	capabilities.DockerAvailable = true
	capabilities.SupportedExecutors = []string{"docker"}

	return capabilities
}

// registerWithControlPlane attempts one startup registration and returns the
// identity the Runner should serve with. It never returns an error: a failed
// or rejected registration is logged and the Runner keeps serving so Ping and
// GetInfo stay reachable for diagnosis, but the returned identity's status
// stays unavailable so the Runner is never silently reported as ready.
func registerWithControlPlane(
	ctx context.Context,
	cfg runnerconfig.Config,
	identity runneridentity.Identity,
) runneridentity.Identity {
	outcome := startupregistration.Apply(ctx, cfg.ControlPlaneAddress, identity, cfg.RegistrationTimeout)

	if outcome.Err != nil {
		log.Printf(
			"Runner registration with the Control Plane at %s failed - runner %s stays unavailable: %v",
			cfg.ControlPlaneAddress,
			cfg.RunnerID,
			outcome.Err,
		)

		return outcome.Identity
	}

	if outcome.Result.Decision == registrationclient.DecisionIdentityConflict {
		log.Printf(
			"Control Plane at %s rejected registration for runner %s - identity conflict - %s",
			cfg.ControlPlaneAddress,
			cfg.RunnerID,
			outcome.Result.Message,
		)

		return outcome.Identity
	}

	log.Printf(
		"Runner %s registered with the Control Plane at %s - registration %s - result %s",
		cfg.RunnerID,
		cfg.ControlPlaneAddress,
		outcome.Result.RegistrationID,
		outcome.Result.Decision,
	)

	return outcome.Identity
}
