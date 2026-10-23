// Package heartbeatclient lets the Runner report liveness to the Control
// Plane's RunnerRegistrationService over gRPC. It mirrors registrationclient:
// a transport failure is returned as an error, and every response the Control
// Plane sends back is turned into a deterministic Decision instead.
package heartbeatclient

import (
	"context"
	"fmt"

	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Decision mirrors the Control Plane's heartbeat decision.
type Decision string

const (
	DecisionUnspecified   Decision = "unspecified"
	DecisionAcknowledged  Decision = "acknowledged"
	DecisionUnknownRunner Decision = "unknown_runner"
)

// Result is the Control Plane's answer to one heartbeat.
type Result struct {
	Decision Decision
	Message  string
}

// Dialer creates the client connection used for one heartbeat attempt. It is
// a seam for tests; production callers use grpc.NewClient.
type Dialer func(address string) (*grpc.ClientConn, error)

// Heartbeat reports that identity is still alive to the Control Plane at
// address. The call is bounded by ctx, which the caller derives with a
// timeout so an unreachable Control Plane cannot block the heartbeat loop.
func Heartbeat(ctx context.Context, address string, identity runneridentity.Identity) (Result, error) {
	return heartbeat(ctx, address, identity, defaultDialer)
}

func heartbeat(ctx context.Context, address string, identity runneridentity.Identity, dial Dialer) (Result, error) {
	connection, err := dial(address)
	if err != nil {
		return Result{}, fmt.Errorf("create control plane connection: %w", err)
	}
	defer connection.Close()

	client := runnerv1.NewRunnerRegistrationServiceClient(connection)

	response, err := client.Heartbeat(ctx, &runnerv1.HeartbeatRequest{
		RunnerId:   identity.RunnerID,
		InstanceId: identity.InstanceID,
	})
	if err != nil {
		return Result{}, fmt.Errorf("send heartbeat to control plane at %s: %w", address, err)
	}

	return Result{
		Decision: decisionFromProto(response.GetResult()),
		Message:  response.GetMessage(),
	}, nil
}

func defaultDialer(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func decisionFromProto(result runnerv1.HeartbeatResult) Decision {
	switch result {
	case runnerv1.HeartbeatResult_HEARTBEAT_ACKNOWLEDGED:
		return DecisionAcknowledged
	case runnerv1.HeartbeatResult_HEARTBEAT_UNKNOWN_RUNNER:
		return DecisionUnknownRunner
	default:
		return DecisionUnspecified
	}
}
