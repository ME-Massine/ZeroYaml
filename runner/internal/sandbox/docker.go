package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	// sourceMountPath is where a file:// repository is mounted, read-only, in
	// the checkout container.
	sourceMountPath = "/source"

	managedLabel       = "io.zeroyaml.managed=true"
	executionLabelName = "io.zeroyaml.execution-id"

	// checkoutScript fetches exactly one revision into the empty workspace.
	// The location and revision arrive as positional parameters, never as
	// script text, so request content is not parsed by the shell. The "--"
	// separator keeps git from reading either value as an option.
	//
	// safe.directory is widened because a bind-mounted source is owned by a
	// host user rather than by the container user. allowAnySHA1InWant lets the
	// local upload-pack serve a commit SHA that no reference points at; remote
	// servers apply their own policy and ignore this client-side setting.
	checkoutScript = `set -eu
git init --quiet .
git -c safe.directory='*' -c uploadpack.allowAnySHA1InWant=true fetch --quiet --depth 1 --no-tags -- "$1" "$2"
git checkout --quiet --detach FETCH_HEAD`

	// checkoutScriptName is $0 for checkoutScript and appears in process
	// listings inside the checkout container.
	checkoutScriptName = "zeroyaml-checkout"

	// maxStandardOutputBytes bounds what one Docker CLI call may return. The
	// commands the sandbox reads (create, wait, version) print one short line.
	maxStandardOutputBytes = 4 << 10

	// maxErrorDetailBytes bounds the Docker CLI diagnostics kept for an error.
	// The tail is kept because Docker prints the actionable reason last.
	maxErrorDetailBytes = 512

	// commandWaitDelay bounds how long a killed Docker CLI process may keep its
	// output pipes open, so a cancelled call always returns.
	commandWaitDelay = 5 * time.Second
)

// resourceNames are the Docker names of one execution's resources.
type resourceNames struct {
	executionID string
	volume      string
	checkout    string
	job         string
}

func newResourceNames(executionID string) resourceNames {
	prefix := "zeroyaml-" + executionID

	return resourceNames{
		executionID: executionID,
		volume:      prefix + "-workspace",
		checkout:    prefix + "-checkout",
		job:         prefix + "-job",
	}
}

func volumeCreateArgs(names resourceNames) []string {
	return []string{
		"volume", "create",
		"--label", managedLabel,
		"--label", executionLabelName + "=" + names.executionID,
		names.volume,
	}
}

// containerCreateArgs holds the options shared by every container: identity
// labels, an image pulled only when missing, and the resource bounds.
// no-new-privileges stops setuid binaries in the image from escalating.
func containerCreateArgs(name string, names resourceNames, limits Limits) []string {
	return []string{
		"create",
		"--name", name,
		"--label", managedLabel,
		"--label", executionLabelName + "=" + names.executionID,
		"--pull", "missing",
		"--security-opt", "no-new-privileges",
		"--memory", strconv.FormatInt(limits.MemoryBytes, 10),
		"--memory-swap", strconv.FormatInt(limits.MemoryBytes, 10),
		"--pids-limit", strconv.FormatInt(limits.PIDs, 10),
		"--mount", "type=volume,source=" + names.volume + ",target=" + workspaceMountPath,
	}
}

func checkoutCreateArgs(spec Spec, names resourceNames) []string {
	args := containerCreateArgs(names.checkout, names, spec.Limits)

	fetchLocation := spec.Source.RemoteURL
	if spec.Source.HostPath != "" {
		args = append(args, "--mount", "type=bind,source="+spec.Source.HostPath+",target="+sourceMountPath+",readonly")
		fetchLocation = "file://" + sourceMountPath
	}

	return append(
		args,
		"--workdir", workspaceMountPath,
		"--entrypoint", "sh",
		spec.CheckoutImage,
		"-c", checkoutScript, checkoutScriptName, fetchLocation, spec.Source.Revision,
	)
}

// jobCreateArgs passes the command as an explicit entrypoint and argument
// vector. Overriding the entrypoint keeps an image's own ENTRYPOINT from
// silently wrapping or replacing the dispatched executable.
func jobCreateArgs(spec Spec, names resourceNames) []string {
	args := containerCreateArgs(names.job, names, spec.Limits)
	args = append(
		args,
		"--log-driver", "local",
		"--log-opt", "max-size=10m",
		"--log-opt", "max-file=1",
		"--log-opt", "compress=false",
		"--workdir", spec.WorkingDirectory,
		"--entrypoint", spec.Command[0],
		spec.Image,
	)

	return append(args, spec.Command[1:]...)
}

// dockerStreamFunc copies Docker's non-TTY stdout/stderr pipes into bounded
// writers. Engine frames retain per-stream byte order; observation order cannot
// reconstruct the command's exact write order across streams.
type dockerStreamFunc func(context.Context, io.Writer, io.Writer, ...string) error

func cliDockerStream(dockerBinary string) dockerStreamFunc {
	return func(ctx context.Context, stdout io.Writer, stderr io.Writer, args ...string) error {
		if len(args) == 0 {
			return errors.New("Docker container identity is required")
		}
		// dial-stdio uses the CLI's configured Docker transport, including named
		// pipes on Windows. Engine frames separate container stderr from CLI
		// diagnostics, so output can be forwarded before the follower exits.
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command
		command := exec.CommandContext(streamCtx, dockerBinary, "system", "dial-stdio")
		command.Stderr = io.Discard
		command.WaitDelay = commandWaitDelay
		request, err := command.StdinPipe()
		if err != nil {
			return errors.New("docker output stream failed")
		}
		defer request.Close()
		pipe, err := command.StdoutPipe()
		if err != nil {
			return errors.New("docker output stream failed")
		}
		defer pipe.Close()
		if err := command.Start(); err != nil {
			return errors.New("docker output stream failed")
		}
		// Keep stdin open while reading: dial-stdio closes the daemon connection
		// when its input reaches EOF, even if the response is still arriving.
		if _, err := io.WriteString(request, "GET /containers/"+url.PathEscape(args[len(args)-1])+"/logs?follow=1&stdout=1&stderr=1 HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n"); err != nil {
			cancel()
			command.Wait()
			return errors.New("docker output stream failed")
		}
		response, readErr := http.ReadResponse(bufio.NewReader(pipe), nil)
		if readErr == nil {
			if response.StatusCode != http.StatusOK {
				readErr = errors.New("docker log request failed")
			} else {
				readErr = copyDockerLogFrames(response.Body, stdout, stderr)
			}
			// A malformed frame must not leave Close draining an open response.
			cancel()
			response.Body.Close()
		}
		cancel()
		waitErr := command.Wait()
		if readErr != nil || (waitErr != nil && ctx.Err() != nil) {
			return errors.New("docker output stream failed")
		}
		return nil
	}
}

// Non-TTY Engine logs use an eight-byte header followed by the raw payload.
// Payloads stream through a fixed buffer; partial payloads are
// delivered before a cancellation or transport failure is returned.
func copyDockerLogFrames(source io.Reader, stdout, stderr io.Writer) error {
	var header [8]byte
	var chunk [4096]byte
	for {
		if _, err := io.ReadFull(source, header[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		var destination io.Writer
		switch header[0] {
		case 1:
			destination = stdout
		case 2:
			destination = stderr
		default:
			return errors.New("invalid Docker stream")
		}
		if header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return errors.New("invalid Docker stream header")
		}
		for remaining := int64(binary.BigEndian.Uint32(header[4:])); remaining > 0; {
			n, readErr := io.ReadFull(source, chunk[:min(remaining, int64(len(chunk)))])
			if n > 0 {
				written, writeErr := destination.Write(chunk[:n])
				if writeErr != nil {
					return writeErr
				}
				if written != n {
					return io.ErrShortWrite
				}
				remaining -= int64(n)
			}
			if readErr != nil {
				return readErr
			}
		}
	}
}

// Probe reports whether the Docker daemon behind dockerBinary is reachable,
// bounded by timeout. It returns the daemon version on success.
func Probe(ctx context.Context, dockerBinary string, timeout time.Duration) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	output, err := cliDocker(dockerBinary)(probeCtx, "version", "--format", "{{.Server.Version}}")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(output), nil
}

// cliDocker runs the Docker CLI directly, without a shell, so every argument
// reaches Docker exactly as built.
//
// The executable is never request content: dockerBinary is the Runner's own
// compile-time Docker CLI name, resolved through PATH exactly as for any other
// Docker CLI user. Request content only ever reaches this call as elements of
// args, which exec passes to Docker as a literal argument vector: no host shell
// parses them, so a value cannot become an additional command. Docker itself is
// kept from reading a value as an option by the argument order built in this
// file and by spec validation, which rejects locations, revisions, working
// directories, and commands that do not fit the accepted shapes.
func cliDocker(dockerBinary string) dockerFunc {
	return func(ctx context.Context, args ...string) (string, error) {
		stdout := &headBuffer{limit: maxStandardOutputBytes}
		stderr := &tailBuffer{limit: maxErrorDetailBytes}

		// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command
		command := exec.CommandContext(ctx, dockerBinary, args...)
		command.Stdout = stdout
		command.Stderr = stderr
		command.WaitDelay = commandWaitDelay

		if err := command.Run(); err != nil {
			detail := strings.TrimSpace(stderr.String())
			if detail == "" {
				return "", fmt.Errorf("docker %s: %w", args[0], err)
			}

			return "", fmt.Errorf("docker %s: %w: %s", args[0], err, detail)
		}

		return stdout.String(), nil
	}
}

// headBuffer keeps the first limit bytes written to it and discards the rest.
// It reports every write as complete so the writing process is never blocked.
type headBuffer struct {
	limit  int
	buffer bytes.Buffer
}

func (b *headBuffer) Write(p []byte) (int, error) {
	if remaining := b.limit - b.buffer.Len(); remaining > 0 {
		b.buffer.Write(p[:min(len(p), remaining)])
	}

	return len(p), nil
}

func (b *headBuffer) String() string {
	return b.buffer.String()
}

// tailBuffer keeps the last limit bytes written to it.
type tailBuffer struct {
	limit int
	data  []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	if overflow := len(b.data) - b.limit; overflow > 0 {
		b.data = append(b.data[:0], b.data[overflow:]...)
	}

	return len(p), nil
}

// String drops a character split by the cut, because the diagnostic travels in
// protobuf string fields that must hold valid UTF-8.
func (b *tailBuffer) String() string {
	return strings.ToValidUTF8(string(b.data), "")
}
