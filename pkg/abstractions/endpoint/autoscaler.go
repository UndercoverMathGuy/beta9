package endpoint

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
)

type endpointAutoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
	LatencyP95Ms      float64
	QueueVelocity     float64
	CheckpointReady   bool
}

func endpointSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	totalRequests, err := i.TaskRepo.TasksInFlight(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		return &endpointAutoscalerSample{
			TotalRequests:     -1,
			CurrentContainers: -1,
		}, err
	}

	state, err := i.State()
	if err != nil {
		return &endpointAutoscalerSample{
			TotalRequests:     int64(totalRequests),
			CurrentContainers: -1,
		}, err
	}

	currentContainers := state.PendingContainers + state.RunningContainers

	sample := &endpointAutoscalerSample{
		TotalRequests:     int64(totalRequests),
		CurrentContainers: int64(currentContainers),
	}

	return sample, nil
}

func endpointLatencySampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	baseSample, err := endpointSampleFunc(i)
	if err != nil {
		return baseSample, err
	}
	metrics, err := i.buffer.GetLatencyMetrics(i.buffer.ctx, i.Workspace.Name, i.buffer.stubId)
	if err != nil {
		return baseSample, err
	}
	baseSample.LatencyP95Ms = metrics.P95Ms
	baseSample.QueueVelocity = metrics.QueueVelocity

	baseSample.CheckpointReady = i.buffer.stubConfig.CheckpointEnabled

	return baseSample, nil
}

func endpointDeploymentScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	// Check for error states in sample
	if s.TotalRequests == -1 || s.CurrentContainers == -1 {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	desiredContainers := 0

	if s.TotalRequests == 0 {
		desiredContainers = 0
	} else {

		tasksPerContainer := int64(1)
		if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.TasksPerContainer > 0 {
			tasksPerContainer = int64(i.StubConfig.Autoscaler.TasksPerContainer)
		}

		desiredContainers = int(s.TotalRequests / int64(tasksPerContainer))
		if s.TotalRequests%int64(tasksPerContainer) > 0 {
			desiredContainers += 1
		}

		maxContainers := uint(1)
		if i.StubConfig.Autoscaler != nil {
			maxContainers = i.StubConfig.Autoscaler.MaxContainers
		}

		// Limit max replicas to either what was set in autoscaler config, or the limit specified on the gateway config (whichever is lower)
		maxReplicas := math.Min(float64(maxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	// Enforce MinContainers floor
	if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.MinContainers > 0 {
		minContainers := int(i.StubConfig.Autoscaler.MinContainers)
		if desiredContainers < minContainers {
			desiredContainers = minContainers
		}
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func latencyAwareEndpointScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	// Check for error states in sample
	if s.TotalRequests == -1 || s.CurrentContainers == -1 {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	autoscaler := i.StubConfig.Autoscaler
	if autoscaler == nil {
		return endpointDeploymentScaleFunc(i, s)
	}

	// Calculate max allowed containers
	maxContainers := int(autoscaler.MaxContainers)
	if maxContainers == 0 {
		maxContainers = 1
	}
	maxReplicas := int(math.Min(float64(maxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas)))

	// Calculate min containers floor
	minContainers := int(autoscaler.MinContainers)

	// Helper to enforce min/max bounds
	clampContainers := func(desired int) int {
		if desired < minContainers {
			desired = minContainers
		}
		if desired > maxReplicas {
			desired = maxReplicas
		}
		return desired
	}

	// Latency trigger: scale up if p95 exceeds threshold
	if autoscaler.LatencyThreshold > 0 && s.LatencyP95Ms > float64(autoscaler.LatencyThreshold) {
		desiredContainers := clampContainers(int(s.CurrentContainers) + 1)
		return &abstractions.AutoscalerResult{
			DesiredContainers: desiredContainers,
			ResultValid:       true,
			CRIUScaling:       autoscaler.EnableCRIUScaling && s.CheckpointReady,
		}
	}

	// Queue velocity trigger: scale up if velocity exceeds threshold
	if autoscaler.QueueVelocity > 0 && s.QueueVelocity > float64(autoscaler.QueueVelocity) {
		desiredContainers := clampContainers(int(s.CurrentContainers) + 1)
		return &abstractions.AutoscalerResult{
			DesiredContainers: desiredContainers,
			ResultValid:       true,
			CRIUScaling:       autoscaler.EnableCRIUScaling && s.CheckpointReady,
		}
	}

	// Fallback to queue-depth scaling (which also enforces MinContainers)
	return endpointDeploymentScaleFunc(i, s)
}

func endpointServeScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	lockKey := common.RedisKeys.SchedulerServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, lockKey).Result()
	if err != nil {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	if exists == 0 {
		desiredContainers = 0
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
