package runnerconfig

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

const (
	defaultGRPCAddress = ":50051"
	defaultRunnerID    = "local-runner"
	defaultVersion     = "0.1.0"

	// defaultShutdownTimeout bounds how long the Runner waits for in-flight RPCs
	// to finish after a termination signal. It leaves room for short RPCs to
	// complete while staying below the ten-second stop grace period that Docker
	// applies by default.
	defaultShutdownTimeout = 5 * time.Second

	// defaultControlPlaneAddress targets the Control Plane registration server
	// started alongside the default Spring Boot dev profile.
	defaultControlPlaneAddress = "localhost:50052"

	// defaultRegistrationTimeout bounds the single startup registration attempt
	// so an unreachable Control Plane cannot delay the Runner from serving.
	defaultRegistrationTimeout = 5 * time.Second

	// defaultHeartbeatInterval controls how often a successfully registered
	// Runner reports liveness to the Control Plane.
	defaultHeartbeatInterval = 5 * time.Second

	// defaultHeartbeatTimeout bounds each individual heartbeat attempt so a
	// slow or unreachable Control Plane cannot stall the next scheduled tick.
	defaultHeartbeatTimeout = 5 * time.Second

	// defaultStatusReportTimeout bounds each execution status report so an
	// unreachable Control Plane cannot hold an execution slot.
	defaultStatusReportTimeout = 5 * time.Second

	// defaultJobImage runs Job commands when the operator names no image. The
	// RunJob contract does not carry an image yet, so this is the image every
	// Job uses on this Runner.
	defaultJobImage = "alpine:3.22"

	// defaultCheckoutImage fetches repositories into the Job workspace. It must
	// provide sh and git.
	defaultCheckoutImage = "alpine/git:v2.49.1"

	// defaultJobTimeout bounds one execution, including the source checkout, so
	// a hung command cannot hold an execution slot indefinitely.
	defaultJobTimeout = 10 * time.Minute

	// defaultMaxConcurrentJobs keeps a local Runner to one container at a time.
	defaultMaxConcurrentJobs = 1

	// maxConcurrentJobsLimit caps the configurable concurrency so a typo cannot
	// let one Runner start an unbounded number of containers.
	maxConcurrentJobsLimit = 16

	envGRPCAddress         = "ZEROYAML_RUNNER_GRPC_ADDRESS"
	envRunnerID            = "ZEROYAML_RUNNER_ID"
	envVersion             = "ZEROYAML_RUNNER_VERSION"
	envShutdownTimeout     = "ZEROYAML_RUNNER_SHUTDOWN_TIMEOUT"
	envControlPlaneAddress = "ZEROYAML_CONTROLPLANE_ADDRESS"
	envRegistrationTimeout = "ZEROYAML_RUNNER_REGISTRATION_TIMEOUT"
	envHeartbeatInterval   = "ZEROYAML_RUNNER_HEARTBEAT_INTERVAL"
	envHeartbeatTimeout    = "ZEROYAML_RUNNER_HEARTBEAT_TIMEOUT"
	envStatusReportTimeout = "ZEROYAML_RUNNER_STATUS_REPORT_TIMEOUT"
	envJobImage            = "ZEROYAML_RUNNER_JOB_IMAGE"
	envCheckoutImage       = "ZEROYAML_RUNNER_CHECKOUT_IMAGE"
	envJobTimeout          = "ZEROYAML_RUNNER_JOB_TIMEOUT"
	envMaxConcurrentJobs   = "ZEROYAML_RUNNER_MAX_CONCURRENT_JOBS"
	envLocalSourceRoot     = "ZEROYAML_RUNNER_LOCAL_SOURCE_ROOT"
	envJobLogsAddress      = "ZEROYAML_JOB_LOGS_ADDRESS"
	envJobLogsCertificate  = "ZEROYAML_JOB_LOGS_CERTIFICATE"
	envJobLogsPrivateKey   = "ZEROYAML_JOB_LOGS_PRIVATE_KEY"
	envJobLogsCA           = "ZEROYAML_JOB_LOGS_CA"
)

// Config contains startup configuration for the Runner process.
type Config struct {
	// JobLogs configures optional authenticated output delivery.
	JobLogs joblogs.Endpoint

	GRPCAddress string
	RunnerID    string
	Version     string

	// ShutdownTimeout bounds the drain phase of a graceful shutdown. When it
	// expires, the remaining RPCs are cancelled so the process always terminates.
	ShutdownTimeout time.Duration

	// ControlPlaneAddress is the Control Plane's Runner registration endpoint.
	ControlPlaneAddress string

	// RegistrationTimeout bounds the single startup registration attempt against
	// the Control Plane.
	RegistrationTimeout time.Duration

	// HeartbeatInterval controls how often a registered Runner reports
	// liveness to the Control Plane.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout bounds each individual heartbeat attempt.
	HeartbeatTimeout time.Duration

	// StatusReportTimeout bounds each execution status report.
	StatusReportTimeout time.Duration

	// JobImage is the Docker image that runs every Job command.
	JobImage string

	// CheckoutImage is the Docker image that fetches the Job repository.
	CheckoutImage string

	// JobTimeout bounds one execution from workspace creation to command exit.
	JobTimeout time.Duration

	// MaxConcurrentJobs is the number of executions this Runner runs at once.
	MaxConcurrentJobs int

	// LocalSourceRoot enables file:// repository locations below this absolute
	// host directory. Empty disables them.
	LocalSourceRoot string
}

// Load reads Runner configuration from environment variables and validates it.
func Load() (Config, error) {
	shutdownTimeout, err := durationFromEnv(envShutdownTimeout, defaultShutdownTimeout)
	if err != nil {
		return Config{}, err
	}

	registrationTimeout, err := durationFromEnv(envRegistrationTimeout, defaultRegistrationTimeout)
	if err != nil {
		return Config{}, err
	}

	heartbeatInterval, err := durationFromEnv(envHeartbeatInterval, defaultHeartbeatInterval)
	if err != nil {
		return Config{}, err
	}

	heartbeatTimeout, err := durationFromEnv(envHeartbeatTimeout, defaultHeartbeatTimeout)
	if err != nil {
		return Config{}, err
	}

	statusReportTimeout, err := durationFromEnv(envStatusReportTimeout, defaultStatusReportTimeout)
	if err != nil {
		return Config{}, err
	}

	jobTimeout, err := durationFromEnv(envJobTimeout, defaultJobTimeout)
	if err != nil {
		return Config{}, err
	}

	maxConcurrentJobs, err := intFromEnv(envMaxConcurrentJobs, defaultMaxConcurrentJobs)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		JobLogs: joblogs.Endpoint{
			Address:     valueFromEnv(envJobLogsAddress, ""),
			Certificate: valueFromEnv(envJobLogsCertificate, ""),
			PrivateKey:  valueFromEnv(envJobLogsPrivateKey, ""),
			CA:          valueFromEnv(envJobLogsCA, ""),
		},
		GRPCAddress:         valueFromEnv(envGRPCAddress, defaultGRPCAddress),
		RunnerID:            valueFromEnv(envRunnerID, defaultRunnerID),
		Version:             valueFromEnv(envVersion, defaultVersion),
		ShutdownTimeout:     shutdownTimeout,
		ControlPlaneAddress: valueFromEnv(envControlPlaneAddress, defaultControlPlaneAddress),
		RegistrationTimeout: registrationTimeout,
		HeartbeatInterval:   heartbeatInterval,
		HeartbeatTimeout:    heartbeatTimeout,
		StatusReportTimeout: statusReportTimeout,
		JobImage:            valueFromEnv(envJobImage, defaultJobImage),
		CheckoutImage:       valueFromEnv(envCheckoutImage, defaultCheckoutImage),
		JobTimeout:          jobTimeout,
		MaxConcurrentJobs:   maxConcurrentJobs,
		LocalSourceRoot:     valueFromEnv(envLocalSourceRoot, ""),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate returns an actionable startup error when configuration is invalid.
func (c Config) Validate() error {
	if strings.TrimSpace(c.GRPCAddress) == "" {
		return fmt.Errorf("%s must not be empty", envGRPCAddress)
	}

	if err := validateTCPAddress(c.GRPCAddress); err != nil {
		return fmt.Errorf("%s must be a TCP address in host:port form, such as :50051 or 127.0.0.1:50051: %w", envGRPCAddress, err)
	}

	if strings.TrimSpace(c.RunnerID) == "" {
		return fmt.Errorf("%s must not be empty", envRunnerID)
	}

	if strings.TrimSpace(c.Version) == "" {
		return fmt.Errorf("%s must not be empty", envVersion)
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envShutdownTimeout, c.ShutdownTimeout)
	}

	if strings.TrimSpace(c.ControlPlaneAddress) == "" {
		return fmt.Errorf("%s must not be empty", envControlPlaneAddress)
	}

	if err := validateTCPAddress(c.ControlPlaneAddress); err != nil {
		return fmt.Errorf("%s must be a TCP address in host:port form, such as localhost:50052: %w", envControlPlaneAddress, err)
	}

	if c.RegistrationTimeout <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envRegistrationTimeout, c.RegistrationTimeout)
	}

	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envHeartbeatInterval, c.HeartbeatInterval)
	}

	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envHeartbeatTimeout, c.HeartbeatTimeout)
	}

	return c.validateExecution()
}

func (c Config) validateExecution() error {
	if c.StatusReportTimeout <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envStatusReportTimeout, c.StatusReportTimeout)
	}

	if strings.TrimSpace(c.JobImage) == "" {
		return fmt.Errorf("%s must name a Docker image, such as alpine:3.22", envJobImage)
	}

	if strings.TrimSpace(c.CheckoutImage) == "" {
		return fmt.Errorf("%s must name a Docker image that provides sh and git, such as alpine/git:v2.49.1", envCheckoutImage)
	}

	if c.JobTimeout <= 0 {
		return fmt.Errorf("%s must be greater than zero, got %s", envJobTimeout, c.JobTimeout)
	}

	if c.MaxConcurrentJobs < 1 || c.MaxConcurrentJobs > maxConcurrentJobsLimit {
		return fmt.Errorf("%s must be between 1 and %d, got %d", envMaxConcurrentJobs, maxConcurrentJobsLimit, c.MaxConcurrentJobs)
	}

	if c.LocalSourceRoot == "" {
		return nil
	}

	if !filepath.IsAbs(c.LocalSourceRoot) {
		return fmt.Errorf("%s must be an absolute directory path, got %q", envLocalSourceRoot, c.LocalSourceRoot)
	}

	info, err := os.Stat(c.LocalSourceRoot)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%s must name an existing directory, got %q", envLocalSourceRoot, c.LocalSourceRoot)
	}

	return nil
}

func valueFromEnv(name string, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return strings.TrimSpace(value)
	}

	return fallback
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	rawValue, isSet := os.LookupEnv(name)
	if !isSet {
		return fallback, nil
	}

	value, err := time.ParseDuration(strings.TrimSpace(rawValue))
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration such as 5s or 500ms: %w", name, err)
	}

	return value, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	rawValue, isSet := os.LookupEnv(name)
	if !isSet {
		return fallback, nil
	}

	value, err := strconv.Atoi(strings.TrimSpace(rawValue))
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number: %w", name, err)
	}

	return value, nil
}

func validateTCPAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}

	if strings.Contains(host, "/") {
		return errors.New("host must not contain a path")
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("port must be numeric: %w", err)
	}

	if portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", portNumber)
	}

	return nil
}
