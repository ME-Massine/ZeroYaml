// Package runnerinfoproto maps the protocol-neutral Runner identity to the
// generated RunnerInfo protocol message. Both the GetInfo RPC handler and the
// Control Plane registration client report the same identity fields, so the
// mapping lives in one place.
package runnerinfoproto

import (
	runnerv1 "github.com/MohamedMBG/ZeroYaml/runner/gen/runner/v1"
	"github.com/MohamedMBG/ZeroYaml/runner/internal/runneridentity"
)

// ToProto builds the RunnerInfo message reported for identity.
func ToProto(identity runneridentity.Identity) *runnerv1.RunnerInfo {
	capabilities := identity.Capabilities

	return &runnerv1.RunnerInfo{
		RunnerId:        identity.RunnerID,
		InstanceId:      identity.InstanceID,
		RunnerVersion:   identity.RunnerVersion,
		ProtocolVersion: identity.ProtocolVersion,
		Capabilities: &runnerv1.RunnerCapabilities{
			OperatingSystem:    capabilities.OperatingSystem,
			Architecture:       capabilities.Architecture,
			DockerAvailable:    capabilities.DockerAvailable,
			SupportedExecutors: capabilities.SupportedExecutors,
			Labels:             capabilities.Labels,
		},
		Status:        statusToProto(identity.Status),
		AcceptingWork: identity.AcceptingWork,
	}
}

func statusToProto(status runneridentity.Status) runnerv1.RunnerStatus {
	switch status {
	case runneridentity.StatusStarting:
		return runnerv1.RunnerStatus_RUNNER_STATUS_STARTING
	case runneridentity.StatusReady:
		return runnerv1.RunnerStatus_RUNNER_STATUS_READY
	case runneridentity.StatusDraining:
		return runnerv1.RunnerStatus_RUNNER_STATUS_DRAINING
	case runneridentity.StatusUnavailable:
		return runnerv1.RunnerStatus_RUNNER_STATUS_UNAVAILABLE
	default:
		return runnerv1.RunnerStatus_RUNNER_STATUS_UNSPECIFIED
	}
}
