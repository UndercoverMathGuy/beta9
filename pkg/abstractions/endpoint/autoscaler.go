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

// isInvalidSample checks if the autoscaler sample contains error states
func isInvalidSample(s *endpointAutoscalerSample) bool {
	return s.TotalRequests == -1 || s.CurrentContainers == -1
}

// invalidAutoscalerResult returns a result indicating invalid sample data
func invalidAutoscalerResult() *abstractions.AutoscalerResult {
	return &abstractions.AutoscalerResult{ResultValid: false}
}

// calculateMaxReplicas computes the effective max replicas from autoscaler config and gateway limits
func calculateMaxReplicas(i *endpointInstance) int {
	maxContainers := uint(1)
	if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.MaxContainers > 0 {
		maxContainers = i.StubConfig.Autoscaler.MaxContainers
	}
	return int(math.Min(float64(maxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas)))
}

func endpointDeploymentScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	if isInvalidSample(s) {
		return invalidAutoscalerResult()
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

		maxReplicas := calculateMaxReplicas(i)
		if desiredContainers > maxReplicas {
			desiredContainers = maxReplicas
		}
	}

	// Enforce MinContainers floor, but cap at maxReplicas to ensure hard limits are respected
	if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.MinContainers > 0 {
		minContainers := int(i.StubConfig.Autoscaler.MinContainers)
		if desiredContainers < minContainers {
			desiredContainers = minContainers
		}
		// Re-apply max ceiling in case MinContainers > MaxReplicas (config error, but respect hard limit)
		maxReplicas := calculateMaxReplicas(i)
		if desiredContainers > maxReplicas {
			desiredContainers = maxReplicas
		}
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func latencyAwareEndpointScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	if isInvalidSample(s) {
		return invalidAutoscalerResult()
	}

	autoscaler := i.StubConfig.Autoscaler
	if autoscaler == nil {
		return endpointDeploymentScaleFunc(i, s)
	}

	// Calculate max allowed containers
	maxReplicas := calculateMaxReplicas(i)

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

	// Scale up if latency p95 exceeds threshold OR queue velocity exceeds threshold
	shouldScaleUp := (autoscaler.LatencyThreshold > 0 && s.LatencyP95Ms > float64(autoscaler.LatencyThreshold)) ||
		(autoscaler.QueueVelocity > 0 && s.QueueVelocity > float64(autoscaler.QueueVelocity))

	if shouldScaleUp {
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
