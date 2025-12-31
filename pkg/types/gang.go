package types

import (
	"fmt"
	"time"
)

type GangRequest struct {
	GroupID           string
	NodeCount         int
	ContainerRequests []*ContainerRequest
	PlacementGroup    string
	EnableEFA         bool
	Timestamp         time.Time
	RetryCount        int
}

type NodeGroup struct {
	GroupID      string
	MachineIds   []string
	WorkerIds    []string
	ContainerIds []string
	Status       NodeGroupStatus
	PoolName     string
	ProviderName string
}

type NodeGroupStatus string

const (
	NodeGroupPending   NodeGroupStatus = "pending"
	NodeGroupReady     NodeGroupStatus = "ready"
	NodeGroupFailed    NodeGroupStatus = "failed"
	NodeGroupCompleted NodeGroupStatus = "completed"
)

// MEDIUM FIX #31: Valid state transitions for NodeGroup
// State machine:
// pending -> ready, failed
// ready -> completed, failed
// failed -> (terminal)
// completed -> (terminal)
var validNodeGroupTransitions = map[NodeGroupStatus][]NodeGroupStatus{
	NodeGroupPending:   {NodeGroupReady, NodeGroupFailed},
	NodeGroupReady:     {NodeGroupCompleted, NodeGroupFailed},
	NodeGroupFailed:    {}, // Terminal state
	NodeGroupCompleted: {}, // Terminal state
}

// CanTransitionTo checks if the transition from current status to new status is valid
func (s NodeGroupStatus) CanTransitionTo(newStatus NodeGroupStatus) bool {
	validTransitions, exists := validNodeGroupTransitions[s]
	if !exists {
		return false
	}
	for _, valid := range validTransitions {
		if valid == newStatus {
			return true
		}
	}
	return false
}

// ValidateTransition returns an error if the transition is invalid
func (s NodeGroupStatus) ValidateTransition(newStatus NodeGroupStatus) error {
	if !s.CanTransitionTo(newStatus) {
		return fmt.Errorf("invalid state transition from %s to %s", s, newStatus)
	}
	return nil
}

// IsTerminal returns true if the status is a terminal state
func (s NodeGroupStatus) IsTerminal() bool {
	return s == NodeGroupFailed || s == NodeGroupCompleted
}
