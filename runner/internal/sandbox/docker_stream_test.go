package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/MohamedMBG/ZeroYaml/runner/internal/joblogs"
)

func TestDockerStreamHelper(t *testing.T) {
	if os.Getenv("ZEROYAML_TEST_DOCKER_STREAM_HELPER") != "1" {
		return
	}
	fmt.Fprint(os.Stdout, "command stdout")
	switch os.Args[len(os.Args)-1] {
	case "success":
		fmt.Fprint(os.Stderr, "command stderr")
	case "failure":
		fmt.Fprint(os.Stderr, "Docker CLI diagnostic")
		os.Exit(1)
	case "large":
		fmt.Fprint(os.Stderr, strings.Repeat("x", joblogs.MaxOutputBytes+2))
	}
	os.Exit(0)
}

func TestDockerStreamOnlyForwardsStderrAfterCLISuccess(t *testing.T) {
	t.Setenv("ZEROYAML_TEST_DOCKER_STREAM_HELPER", "1")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name       string
		wantStderr string
		wantError  bool
	}{
		{name: "success", wantStderr: "command stderr"},
		{name: "failure", wantError: true},
		{name: "large", wantStderr: strings.Repeat("x", joblogs.MaxOutputBytes+1)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := cliDockerStream(binary)(context.Background(), &stdout, &stderr, "-test.run=^TestDockerStreamHelper$", "--", scenario.name)
			if (err != nil) != scenario.wantError || stdout.String() != "command stdout" || stderr.String() != scenario.wantStderr {
				t.Fatalf("error = %v, stdout = %q, stderr length = %d", err, stdout.String(), stderr.Len())
			}
		})
	}
}
