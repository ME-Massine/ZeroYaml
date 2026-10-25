package runnerconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadUsesSafeLocalDefaults(t *testing.T) {
	unsetForTest(t, envGRPCAddress)
	unsetForTest(t, envRunnerID)
	unsetForTest(t, envVersion)
	unsetForTest(t, envShutdownTimeout)
	unsetForTest(t, envControlPlaneAddress)
	unsetForTest(t, envRegistrationTimeout)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected defaults to load: %v", err)
	}

	if cfg.GRPCAddress != defaultGRPCAddress {
		t.Fatalf("expected gRPC address %q, got %q", defaultGRPCAddress, cfg.GRPCAddress)
	}
	if cfg.RunnerID != defaultRunnerID {
		t.Fatalf("expected runner ID %q, got %q", defaultRunnerID, cfg.RunnerID)
	}
	if cfg.Version != defaultVersion {
		t.Fatalf("expected version %q, got %q", defaultVersion, cfg.Version)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("expected shutdown timeout %s, got %s", defaultShutdownTimeout, cfg.ShutdownTimeout)
	}
	if cfg.ControlPlaneAddress != defaultControlPlaneAddress {
		t.Fatalf("expected control plane address %q, got %q", defaultControlPlaneAddress, cfg.ControlPlaneAddress)
	}
	if cfg.RegistrationTimeout != defaultRegistrationTimeout {
		t.Fatalf("expected registration timeout %s, got %s", defaultRegistrationTimeout, cfg.RegistrationTimeout)
	}
}

func TestLoadReadsEnvironmentOverrides(t *testing.T) {
	t.Setenv(envGRPCAddress, "127.0.0.1:51001")
	t.Setenv(envRunnerID, "developer-runner")
	t.Setenv(envVersion, "1.2.3")
	t.Setenv(envShutdownTimeout, "250ms")
	t.Setenv(envControlPlaneAddress, "127.0.0.1:51002")
	t.Setenv(envRegistrationTimeout, "500ms")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected environment overrides to load: %v", err)
	}

	if cfg.GRPCAddress != "127.0.0.1:51001" {
		t.Fatalf("expected override gRPC address, got %q", cfg.GRPCAddress)
	}
	if cfg.RunnerID != "developer-runner" {
		t.Fatalf("expected override runner ID, got %q", cfg.RunnerID)
	}
	if cfg.Version != "1.2.3" {
		t.Fatalf("expected override version, got %q", cfg.Version)
	}
	if cfg.ShutdownTimeout != 250*time.Millisecond {
		t.Fatalf("expected override shutdown timeout, got %s", cfg.ShutdownTimeout)
	}
	if cfg.ControlPlaneAddress != "127.0.0.1:51002" {
		t.Fatalf("expected override control plane address, got %q", cfg.ControlPlaneAddress)
	}
	if cfg.RegistrationTimeout != 500*time.Millisecond {
		t.Fatalf("expected override registration timeout, got %s", cfg.RegistrationTimeout)
	}
}

func TestLoadRejectsInvalidControlPlaneAddress(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		wantError string
	}{
		{name: "empty", address: " ", wantError: envControlPlaneAddress},
		{name: "missing port", address: "localhost", wantError: envControlPlaneAddress},
		{name: "non numeric port", address: "localhost:http", wantError: "port must be numeric"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envGRPCAddress, defaultGRPCAddress)
			t.Setenv(envRunnerID, defaultRunnerID)
			t.Setenv(envVersion, defaultVersion)
			t.Setenv(envControlPlaneAddress, tt.address)

			_, err := Load()
			if err == nil {
				t.Fatal("expected an invalid control plane address to fail")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestLoadRejectsInvalidRegistrationTimeout(t *testing.T) {
	tests := []struct {
		name                string
		registrationTimeout string
		wantError           string
	}{
		{name: "not a duration", registrationTimeout: "soon", wantError: "must be a Go duration"},
		{name: "zero", registrationTimeout: "0s", wantError: "must be greater than zero"},
		{name: "negative", registrationTimeout: "-1s", wantError: "must be greater than zero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envGRPCAddress, defaultGRPCAddress)
			t.Setenv(envRunnerID, defaultRunnerID)
			t.Setenv(envVersion, defaultVersion)
			t.Setenv(envRegistrationTimeout, tt.registrationTimeout)

			_, err := Load()
			if err == nil {
				t.Fatal("expected an invalid registration timeout to fail")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestLoadRejectsInvalidShutdownTimeout(t *testing.T) {
	tests := []struct {
		name            string
		shutdownTimeout string
		wantError       string
	}{
		{
			name:            "not a duration",
			shutdownTimeout: "soon",
			wantError:       "must be a Go duration",
		},
		{
			name:            "missing unit",
			shutdownTimeout: "5",
			wantError:       "must be a Go duration",
		},
		{
			name:            "zero",
			shutdownTimeout: "0s",
			wantError:       "must be greater than zero",
		},
		{
			name:            "negative",
			shutdownTimeout: "-1s",
			wantError:       "must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envGRPCAddress, defaultGRPCAddress)
			t.Setenv(envRunnerID, defaultRunnerID)
			t.Setenv(envVersion, defaultVersion)
			t.Setenv(envShutdownTimeout, tt.shutdownTimeout)

			_, err := Load()
			if err == nil {
				t.Fatal("expected an invalid shutdown timeout to fail")
			}
			if !strings.Contains(err.Error(), envShutdownTimeout) {
				t.Fatalf("expected the error to name %s, got %q", envShutdownTimeout, err.Error())
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func unsetForTest(t *testing.T, name string) {
	t.Helper()

	previousValue, wasSet := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("failed to unset %s: %v", name, err)
	}

	t.Cleanup(func() {
		if wasSet {
			if err := os.Setenv(name, previousValue); err != nil {
				t.Fatalf("failed to restore %s: %v", name, err)
			}
			return
		}

		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("failed to keep %s unset: %v", name, err)
		}
	})
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		runnerID  string
		version   string
		wantError string
	}{
		{
			name:      "empty address",
			address:   " ",
			runnerID:  defaultRunnerID,
			version:   defaultVersion,
			wantError: envGRPCAddress,
		},
		{
			name:      "missing port",
			address:   "localhost",
			runnerID:  defaultRunnerID,
			version:   defaultVersion,
			wantError: envGRPCAddress,
		},
		{
			name:      "non numeric port",
			address:   "localhost:http",
			runnerID:  defaultRunnerID,
			version:   defaultVersion,
			wantError: "port must be numeric",
		},
		{
			name:      "port out of range",
			address:   ":70000",
			runnerID:  defaultRunnerID,
			version:   defaultVersion,
			wantError: "port must be between 1 and 65535",
		},
		{
			name:      "empty runner ID",
			address:   defaultGRPCAddress,
			runnerID:  " ",
			version:   defaultVersion,
			wantError: envRunnerID,
		},
		{
			name:      "empty version",
			address:   defaultGRPCAddress,
			runnerID:  defaultRunnerID,
			version:   " ",
			wantError: envVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envGRPCAddress, tt.address)
			t.Setenv(envRunnerID, tt.runnerID)
			t.Setenv(envVersion, tt.version)

			_, err := Load()
			if err == nil {
				t.Fatal("expected invalid configuration to fail")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestLoadUsesSafeExecutionDefaults(t *testing.T) {
	for _, name := range []string{
		envStatusReportTimeout, envJobImage, envCheckoutImage, envJobTimeout, envMaxConcurrentJobs, envLocalSourceRoot,
	} {
		unsetForTest(t, name)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected defaults to load: %v", err)
	}

	if cfg.StatusReportTimeout != defaultStatusReportTimeout {
		t.Errorf("expected status report timeout %s, got %s", defaultStatusReportTimeout, cfg.StatusReportTimeout)
	}
	if cfg.JobImage != defaultJobImage {
		t.Errorf("expected job image %q, got %q", defaultJobImage, cfg.JobImage)
	}
	if cfg.CheckoutImage != defaultCheckoutImage {
		t.Errorf("expected checkout image %q, got %q", defaultCheckoutImage, cfg.CheckoutImage)
	}
	if cfg.JobTimeout != defaultJobTimeout {
		t.Errorf("expected job timeout %s, got %s", defaultJobTimeout, cfg.JobTimeout)
	}
	if cfg.MaxConcurrentJobs != defaultMaxConcurrentJobs {
		t.Errorf("expected %d concurrent jobs, got %d", defaultMaxConcurrentJobs, cfg.MaxConcurrentJobs)
	}
	if cfg.LocalSourceRoot != "" {
		t.Errorf("expected file:// sources to be disabled by default, got root %q", cfg.LocalSourceRoot)
	}
}

func TestLoadReadsExecutionOverrides(t *testing.T) {
	sourceRoot := t.TempDir()
	t.Setenv(envStatusReportTimeout, "2s")
	t.Setenv(envJobImage, "golang:1.27.1")
	t.Setenv(envCheckoutImage, "alpine/git:v2.49.1")
	t.Setenv(envJobTimeout, "90s")
	t.Setenv(envMaxConcurrentJobs, "4")
	t.Setenv(envLocalSourceRoot, sourceRoot)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected execution overrides to load: %v", err)
	}

	if cfg.StatusReportTimeout != 2*time.Second {
		t.Errorf("expected override status report timeout, got %s", cfg.StatusReportTimeout)
	}
	if cfg.JobImage != "golang:1.27.1" {
		t.Errorf("expected override job image, got %q", cfg.JobImage)
	}
	if cfg.JobTimeout != 90*time.Second {
		t.Errorf("expected override job timeout, got %s", cfg.JobTimeout)
	}
	if cfg.MaxConcurrentJobs != 4 {
		t.Errorf("expected override concurrency, got %d", cfg.MaxConcurrentJobs)
	}
	if cfg.LocalSourceRoot != sourceRoot {
		t.Errorf("expected override local source root, got %q", cfg.LocalSourceRoot)
	}
}

func TestLoadRejectsInvalidExecutionConfiguration(t *testing.T) {
	missingDirectory := filepath.Join(t.TempDir(), "missing")

	tests := []struct {
		name      string
		variable  string
		value     string
		wantError string
	}{
		{name: "zero job timeout", variable: envJobTimeout, value: "0s", wantError: "must be greater than zero"},
		{name: "unparsable job timeout", variable: envJobTimeout, value: "forever", wantError: "must be a Go duration"},
		{name: "zero status report timeout", variable: envStatusReportTimeout, value: "0s", wantError: "must be greater than zero"},
		{name: "empty job image", variable: envJobImage, value: " ", wantError: "must name a Docker image"},
		{name: "empty checkout image", variable: envCheckoutImage, value: " ", wantError: "must name a Docker image"},
		{name: "zero concurrency", variable: envMaxConcurrentJobs, value: "0", wantError: "must be between 1 and 16"},
		{name: "excessive concurrency", variable: envMaxConcurrentJobs, value: "17", wantError: "must be between 1 and 16"},
		{name: "non numeric concurrency", variable: envMaxConcurrentJobs, value: "many", wantError: "must be a whole number"},
		{name: "relative source root", variable: envLocalSourceRoot, value: "sources", wantError: "must be an absolute directory path"},
		{name: "missing source root", variable: envLocalSourceRoot, value: missingDirectory, wantError: "must name an existing directory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.variable, tt.value)

			_, err := Load()
			if err == nil {
				t.Fatalf("expected %s=%q to fail", tt.variable, tt.value)
			}
			if !strings.Contains(err.Error(), tt.variable) {
				t.Errorf("expected the error to name %s, got %q", tt.variable, err.Error())
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestLoadReadsOptionalJobLogTLSConfiguration(t *testing.T) {
	for _, name := range []string{"ZEROYAML_JOB_LOGS_ADDRESS", "ZEROYAML_JOB_LOGS_CERTIFICATE", "ZEROYAML_JOB_LOGS_PRIVATE_KEY", "ZEROYAML_JOB_LOGS_CA"} {
		unsetForTest(t, name)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JobLogs.Address != "" || cfg.JobLogs.Certificate != "" || cfg.JobLogs.PrivateKey != "" || cfg.JobLogs.CA != "" {
		t.Fatal("log delivery must be disabled by default")
	}
	t.Setenv("ZEROYAML_JOB_LOGS_ADDRESS", "localhost:50053")
	t.Setenv("ZEROYAML_JOB_LOGS_CERTIFICATE", "runner.pem")
	t.Setenv("ZEROYAML_JOB_LOGS_PRIVATE_KEY", "runner-key.pem")
	t.Setenv("ZEROYAML_JOB_LOGS_CA", "ca.pem")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JobLogs.Address != "localhost:50053" || cfg.JobLogs.Certificate != "runner.pem" || cfg.JobLogs.PrivateKey != "runner-key.pem" || cfg.JobLogs.CA != "ca.pem" {
		t.Fatal("log TLS configuration was not loaded")
	}
}
