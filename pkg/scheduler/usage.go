package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SchedulerUsageMetrics struct {
	UsageRepo repository.UsageMetricsRepository
}

func NewSchedulerUsageMetrics(usageMetricsRepo repository.UsageMetricsRepository) SchedulerUsageMetrics {
	return SchedulerUsageMetrics{
		UsageRepo: usageMetricsRepo,
	}
}

func (sm *SchedulerUsageMetrics) CounterIncContainerScheduled(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerContainerScheduled, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerUsageMetrics) CounterIncContainerRequested(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerContainerRequested, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerUsageMetrics) CounterIncClusterRequested(request *types.GangRequest) {
	if sm.UsageRepo == nil {
		return
	}

	var workspaceId, stubId, gpu string
	if len(request.ContainerRequests) > 0{
		workspaceId = request.ContainerRequests[0].WorkspaceId
		stubId = request.ContainerRequests[0].StubId
		gpu = request.ContainerRequests[0].Gpu
	}
	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerClusterRequested, map[string]interface{}{
		"value": 1,
		"workspace_id": workspaceId,
		"stub_id": stubId,
		"gpu": gpu,
		"node_count": request.NodeCount,
		"group_id": request.GroupID,
	}, 1.0)
}

func (sm *SchedulerUsageMetrics) CounterIncClusterScheduled(request *types.GangRequest) {
	if sm.UsageRepo == nil {
		return
	}

	var workspaceId, stubId, gpu string
	if len(request.ContainerRequests) > 0{
		workspaceId = request.ContainerRequests[0].WorkspaceId
		stubId = request.ContainerRequests[0].StubId
		gpu = request.ContainerRequests[0].Gpu
	}
	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerClusterScheduled, map[string]interface{}{
		"value": 1,
		"workspace_id": workspaceId,
		"stub_id": stubId,
		"gpu": gpu,
		"node_count": request.NodeCount,
		"group_id": request.GroupID,
	}, 1.0)
}