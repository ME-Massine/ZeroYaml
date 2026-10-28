package sandbox

import (
	"slices"
	"testing"
)

func TestJobLogDriverOnlyOverriddenWhenDeliveryEnabled(t *testing.T) {
	names := newResourceNames(testExecutionID)
	disabled := jobCreateArgs(testSpec(), names, false)
	if slices.Contains(disabled, "--log-driver") || slices.Contains(disabled, "--log-opt") {
		t.Fatal("disabled delivery overrides daemon logging")
	}
	enabled := jobCreateArgs(testSpec(), names, true)
	assertOption(t, enabled, "--log-driver", "local")
	for _, option := range []string{"max-size=10m", "max-file=1", "compress=false"} {
		assertOption(t, enabled, "--log-opt", option)
	}
}
