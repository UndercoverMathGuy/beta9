package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	gangHealthCheckInterval = 15 * time.Second
	// Maximum time a container can remain in PENDING state before being considered stuck
	gangContainerPendingTimeout = 10 * time.Minute
	// Maximum time a container can run before being considered timed out (default, can be overridden)
	gangContainerRunningTimeout = 24 * time.Hour
)

type GangHealthMonitor struct {
	ctx           context.Context
	nodeGroupRepo repository.NodeGroupRepository
	providerRepo  repository.ProviderRepository
	containerRepo repository.ContainerRepository
	eventRepo     repository.EventRepository
}

func NewGangHealthMonitor(
	ctx context.Context,
	nodeGroupRepo repository.NodeGroupRepository,
	providerRepo repository.ProviderRepository,
	containerRepo repository.ContainerRepository,
	eventRepo repository.EventRepository,
) *GangHealthMonitor {
	return &GangHealthMonitor{
		ctx:           ctx,
		nodeGroupRepo: nodeGroupRepo,
		providerRepo:  providerRepo,
		containerRepo: containerRepo,
		eventRepo:     eventRepo,
	}
}

func (m *GangHealthMonitor) Start() {
	ticker := time.NewTicker(gangHealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkAllNodeGroups()
		}
	}
}

func (m *GangHealthMonitor) checkAllNodeGroups() {
	nodeGroups, err := m.nodeGroupRepo.GetAllNodeGroups()
	if err != nil {
		log.Error().Err(err).Msg("Failed to list all node groups")
		return
	}
	for _, ng := range nodeGroups {
		if ng.Status == types.NodeGroupFailed {
			continue
		}
		m.checkNodeGroup(ng)
	}
}

func (m *GangHealthMonitor) checkNodeGroup(ng *types.NodeGroup) {
	for _, machineId := range ng.MachineIds {
		machine, err := m.providerRepo.GetMachine(ng.ProviderName, ng.PoolName, machineId)
		if err != nil || machine == nil {
			log.Warn().Str("GroupID", ng.GroupID).Str("MachineID", machineId).Msg("Node machine failed, killing entire cluster")
			m.killGang(ng, machineId)
			return
		}
	}

	allCompleted := true
	now := time.Now().Unix()

	for _, containerId := range ng.ContainerIds {
		state, err := m.containerRepo.GetContainerState(containerId)
		if err != nil {
			// Container state not found - likely already cleaned up after completion
			// This is a valid terminal state
			continue
		}

		// Check for timeout based on container status
		switch state.Status {
		case types.ContainerStatusPending:
			// Check if container has been pending too long (stuck in scheduling)
			if state.ScheduledAt > 0 {
				pendingDuration := time.Duration(now-state.ScheduledAt) * time.Second
				if pendingDuration > gangContainerPendingTimeout {
					log.Warn().
						Str("GroupID", ng.GroupID).
						Str("ContainerID", containerId).
						Dur("pending_duration", pendingDuration).
						Msg("Container stuck in pending state, killing cluster")
					m.killGang(ng, fmt.Sprintf("container_pending_timeout:%s", containerId))
					return
				}
			}
			allCompleted = false

		case types.ContainerStatusRunning:
			// Check if container has been running too long
			if state.StartedAt > 0 {
				runningDuration := time.Duration(now-state.StartedAt) * time.Second
				if runningDuration > gangContainerRunningTimeout {
					log.Warn().
						Str("GroupID", ng.GroupID).
						Str("ContainerID", containerId).
						Dur("running_duration", runningDuration).
						Msg("Container exceeded maximum runtime, killing cluster")
					m.killGang(ng, fmt.Sprintf("container_timeout:%s", containerId))
					return
				}
			}
			allCompleted = false

		case types.ContainerStatusStopping:
			// Container is transitioning to completion - this is fine
		}
	}

	if allCompleted {
		m.completeGang(ng)
	}
}

func (m *GangHealthMonitor) killGang(ng *types.NodeGroup, failedMachineId string) {
	for _, containerId := range ng.ContainerIds {
		err := m.containerRepo.UpdateContainerStatus(containerId, types.ContainerStatusStopping, 0)
		if err != nil {
			log.Error().Err(err).Str("ContainerId", containerId).Msg("Failed to stop container")
		}
	}
	m.nodeGroupRepo.UpdateNodeGroupStatus(ng.GroupID, types.NodeGroupFailed)

	failureReason := fmt.Sprintf("machine_failure:%s", failedMachineId)
	m.eventRepo.PushClusterFailedEvent(ng.GroupID, failureReason)
	log.Info().Str("GroupID", ng.GroupID).Str("failed_machine", failedMachineId).Msg("Cluster killed due to machine failure")
}

func (m *GangHealthMonitor) completeGang(ng *types.NodeGroup) {
	m.nodeGroupRepo.DeleteNodeGroup(ng.GroupID)
	m.eventRepo.PushClusterCompletedEvent(ng.GroupID)
	log.Info().Str("GroupID", ng.GroupID).Msg("Cluster completed successfully")
}
