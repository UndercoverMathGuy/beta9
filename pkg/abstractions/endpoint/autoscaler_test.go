package endpoint

import (
	"context"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

// Positive: Basic latency autoscaler scales correctly with typical config
func TestLatencyAwareScaleFunc_Positive_BasicScaling(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		MinContainers:     1,
		TasksPerContainer: 1,
		LatencyThreshold:  100,
	})

	// Normal operation: 5 requests, 2 containers, latency OK
	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      50, // Below threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should scale to match request count")
}

// Positive: Queue-depth autoscaler scales correctly with typical config
func TestDeploymentScaleFunc_Positive_BasicScaling(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     10,
		TasksPerContainer: 2,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 3,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// 10 requests / 2 tasks per container = 5 containers
	assert.Equal(t, 5, result.DesiredContainers)
}

// Positive: Latency trigger correctly scales up under load
func TestLatencyAwareScaleFunc_Positive_LatencyTriggeredScaleUp(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		LatencyThreshold: 100,
	})

	// High latency detected, should scale up by 1
	sample := &endpointAutoscalerSample{
		TotalRequests:     20,
		CurrentContainers: 3,
		LatencyP95Ms:      150, // Exceeds 100ms threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 4, result.DesiredContainers, "should scale up by 1 when latency high")
}

// Positive: Queue velocity trigger correctly scales up during traffic spike
func TestLatencyAwareScaleFunc_Positive_VelocityTriggeredScaleUp(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 10,
		QueueVelocity: 20,
	})

	// Traffic spike detected via queue velocity
	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 2,
		QueueVelocity:     50, // Exceeds 20 threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers, "should scale up by 1 when velocity high")
}

// Positive: Scale down when queue empties
func TestDeploymentScaleFunc_Positive_ScaleDownOnEmptyQueue(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.QueueDepthAutoscaler,
		MaxContainers: 10,
		MinContainers: 0,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     0,
		CurrentContainers: 5,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers, "should scale to 0 when no requests")
}

// Positive: Maintains MinContainers during low traffic
func TestDeploymentScaleFunc_Positive_MaintainsMinDuringLowTraffic(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.QueueDepthAutoscaler,
		MaxContainers: 10,
		MinContainers: 2,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     1,
		CurrentContainers: 5,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers, "should maintain MinContainers")
}

// Positive: Respects MaxContainers during high traffic
func TestDeploymentScaleFunc_Positive_RespectsMaxDuringHighTraffic(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     5,
		TasksPerContainer: 1,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     100,
		CurrentContainers: 3,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should cap at MaxContainers")
}

// Positive: CRIU scaling enabled correctly when conditions met
func TestLatencyAwareScaleFunc_Positive_CRIUScalingWorks(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		LatencyThreshold:  100,
		EnableCRIUScaling: true,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 2,
		LatencyP95Ms:      200,
		CheckpointReady:   true,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.True(t, result.CRIUScaling, "CRIU scaling should be enabled")
	assert.Equal(t, 3, result.DesiredContainers)
}

// Positive: TasksPerContainer correctly batches requests
func TestDeploymentScaleFunc_Positive_TasksPerContainerBatching(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     20,
		TasksPerContainer: 5,
	})

	// 23 requests / 5 tasks per container = 4.6 -> 5 containers
	sample := &endpointAutoscalerSample{
		TotalRequests:     23,
		CurrentContainers: 2,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should round up for partial batches")
}

// Positive: Exact division with TasksPerContainer
func TestDeploymentScaleFunc_Positive_ExactDivision(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     20,
		TasksPerContainer: 5,
	})

	// 20 requests / 5 tasks per container = 4 containers exactly
	sample := &endpointAutoscalerSample{
		TotalRequests:     20,
		CurrentContainers: 2,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 4, result.DesiredContainers, "should not round up for exact division")
}

// =============================================================================
// IDEAL FLOW TESTS - End-to-End Scenarios
// =============================================================================

// IdealFlow: Complete scaling lifecycle from cold start to scale down
func TestAutoscaler_IdealFlow_ColdStartToScaleDown(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		MinContainers:     1,
		TasksPerContainer: 2,
		LatencyThreshold:  100,
	})

	// Phase 1: Cold start - no containers, first request arrives
	sample := &endpointAutoscalerSample{
		TotalRequests:     1,
		CurrentContainers: 0,
		LatencyP95Ms:      0,
	}
	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers, "Phase 1: should scale to 1 for first request")

	// Phase 2: Traffic ramp up - more requests, latency still OK
	sample = &endpointAutoscalerSample{
		TotalRequests:     8,
		CurrentContainers: 1,
		LatencyP95Ms:      50,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 4, result.DesiredContainers, "Phase 2: should scale based on queue depth (8/2=4)")

	// Phase 3: High load - latency spikes, trigger latency-based scaling
	sample = &endpointAutoscalerSample{
		TotalRequests:     8,
		CurrentContainers: 4,
		LatencyP95Ms:      150,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "Phase 3: should scale up by 1 due to latency")

	// Phase 4: Traffic subsides - queue empties, scale down
	sample = &endpointAutoscalerSample{
		TotalRequests:     0,
		CurrentContainers: 5,
		LatencyP95Ms:      20,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers, "Phase 4: should scale down to MinContainers")
}

// IdealFlow: Burst traffic handling with velocity trigger
func TestAutoscaler_IdealFlow_BurstTrafficHandling(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 10,
		MinContainers: 2,
		QueueVelocity: 30,
	})

	// Steady state
	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 2,
		QueueVelocity:     10,
	}
	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid, "Steady: result should be valid")
	assert.Equal(t, 10, result.DesiredContainers, "Steady: queue-depth scaling")

	// Burst detected via velocity
	sample = &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 10,
		QueueVelocity:     50, // Spike!
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid, "Burst: result should be valid")
	assert.Equal(t, 10, result.DesiredContainers, "Burst: already at max, can't scale further")

	// Burst subsides
	sample = &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 10,
		QueueVelocity:     5,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid, "Post-burst: result should be valid")
	assert.Equal(t, 5, result.DesiredContainers, "Post-burst: scale down to match queue")
}

// =============================================================================
// ATOMIC TESTS - Edge Cases
// =============================================================================

// Atom 1: Error state detection - TotalRequests == -1
func TestLatencyAwareScaleFunc_Atom_ErrorStateTotalRequests(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 5,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     -1,
		CurrentContainers: 2,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.False(t, result.ResultValid, "should return invalid when TotalRequests == -1")
}

// Atom 1b: Error state detection - CurrentContainers == -1
func TestLatencyAwareScaleFunc_Atom_ErrorStateCurrentContainers(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 5,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: -1,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.False(t, result.ResultValid, "should return invalid when CurrentContainers == -1")
}

// Atom 2: Nil autoscaler fallback - delegates to endpointDeploymentScaleFunc
func TestLatencyAwareScaleFunc_Atom_NilAutoscalerFallback(t *testing.T) {
	instance := createTestInstance(t, nil)

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 1,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// With nil autoscaler, endpointDeploymentScaleFunc uses default TasksPerContainer=1
	// and MaxContainers=1, so 5 requests -> capped at 1
	assert.Equal(t, 1, result.DesiredContainers)
}

// Atom 3: Max containers calculation - defaults to 1 when MaxContainers == 0
func TestLatencyAwareScaleFunc_Atom_MaxContainersDefaultsToOne(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    0, // Should default to 1
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 0,
		LatencyP95Ms:      200, // Exceeds threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers, "should cap at 1 when MaxContainers defaults to 1")
}

// Atom 3b: Max containers respects gateway StubLimits.MaxReplicas
func TestLatencyAwareScaleFunc_Atom_MaxContainersRespectsGatewayLimit(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    100, // High limit
		LatencyThreshold: 100,
	})
	// Gateway limit is 10 (set in createTestInstance)

	sample := &endpointAutoscalerSample{
		TotalRequests:     50,
		CurrentContainers: 9,
		LatencyP95Ms:      200, // Exceeds threshold, wants to scale to 10
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 10, result.DesiredContainers, "should cap at gateway MaxReplicas (10)")
}

// Atom 4: Min containers floor - clampContainers enforces minimum
func TestLatencyAwareScaleFunc_Atom_MinContainersFloor(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		MinContainers:    3,
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     1,
		CurrentContainers: 0,
		LatencyP95Ms:      200, // Exceeds threshold, wants to scale to 1
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers, "should enforce MinContainers floor")
}

// Atom 5: Max containers ceiling - clampContainers enforces maximum
func TestLatencyAwareScaleFunc_Atom_MaxContainersCeiling(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    5,
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     100,
		CurrentContainers: 10,  // Already above max
		LatencyP95Ms:      200, // Exceeds threshold, wants to scale to 11
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should cap at MaxContainers")
}

// Atom 6: Latency threshold trigger - scales up when LatencyP95Ms > LatencyThreshold
func TestLatencyAwareScaleFunc_Atom_LatencyThresholdTrigger(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      150, // Exceeds 100ms threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers, "should scale up by 1 when latency exceeds threshold")
}

// Atom 6b: Latency threshold - no trigger when below threshold
func TestLatencyAwareScaleFunc_Atom_LatencyThresholdNoTrigger(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      50, // Below threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// Falls back to queue-depth: 5 requests / 1 task per container = 5 containers
	assert.Equal(t, 5, result.DesiredContainers, "should fallback to queue-depth when latency is OK")
}

// Atom 6c: Latency threshold - no trigger when threshold is 0 (disabled)
func TestLatencyAwareScaleFunc_Atom_LatencyThresholdDisabled(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		LatencyThreshold: 0, // Disabled
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      9999, // Very high but threshold is disabled
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// Falls back to queue-depth
	assert.Equal(t, 5, result.DesiredContainers)
}

// Atom 7: Queue velocity trigger - scales up when QueueVelocity > threshold
func TestLatencyAwareScaleFunc_Atom_QueueVelocityTrigger(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 10,
		QueueVelocity: 50,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		QueueVelocity:     100, // Exceeds 50 threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers, "should scale up by 1 when velocity exceeds threshold")
}

// Atom 7b: Queue velocity - no trigger when below threshold
func TestLatencyAwareScaleFunc_Atom_QueueVelocityNoTrigger(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.LatencyAutoscaler,
		MaxContainers: 10,
		QueueVelocity: 50,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		QueueVelocity:     30, // Below threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// Falls back to queue-depth
	assert.Equal(t, 5, result.DesiredContainers)
}

// Atom 8: CRIU scaling flag - set when EnableCRIUScaling && CheckpointReady
func TestLatencyAwareScaleFunc_Atom_CRIUScalingEnabled(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		LatencyThreshold:  100,
		EnableCRIUScaling: true,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      200,
		CheckpointReady:   true,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.True(t, result.CRIUScaling, "CRIUScaling should be true when enabled and checkpoint ready")
}

// Atom 8b: CRIU scaling flag - not set when checkpoint not ready
func TestLatencyAwareScaleFunc_Atom_CRIUScalingCheckpointNotReady(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		LatencyThreshold:  100,
		EnableCRIUScaling: true,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      200,
		CheckpointReady:   false,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.False(t, result.CRIUScaling, "CRIUScaling should be false when checkpoint not ready")
}

// Atom 8c: CRIU scaling flag - not set when EnableCRIUScaling is false
func TestLatencyAwareScaleFunc_Atom_CRIUScalingDisabled(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		LatencyThreshold:  100,
		EnableCRIUScaling: false,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      200,
		CheckpointReady:   true,
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.False(t, result.CRIUScaling, "CRIUScaling should be false when disabled")
}

// Atom 9: Fallback to queue-depth - when no triggers fire
func TestLatencyAwareScaleFunc_Atom_FallbackToQueueDepth(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.LatencyAutoscaler,
		MaxContainers:     10,
		TasksPerContainer: 2,
		LatencyThreshold:  100,
		QueueVelocity:     50,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 2,
		LatencyP95Ms:      50, // Below threshold
		QueueVelocity:     30, // Below threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// Queue-depth: 10 requests / 2 tasks per container = 5 containers
	assert.Equal(t, 5, result.DesiredContainers, "should use queue-depth calculation when no triggers fire")
}

// =============================================================================
// ATOMIC TESTS FOR endpointDeploymentScaleFunc (queue-depth)
// =============================================================================

// Atom: MinContainers floor in queue-depth scaling
func TestDeploymentScaleFunc_Atom_MinContainersFloor(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:          types.QueueDepthAutoscaler,
		MaxContainers: 10,
		MinContainers: 2,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     0, // No requests, would normally scale to 0
		CurrentContainers: 5,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers, "should enforce MinContainers even with 0 requests")
}

// Atom: MinContainers floor when requests < min
func TestDeploymentScaleFunc_Atom_MinContainersFloorWithRequests(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     10,
		MinContainers:     5,
		TasksPerContainer: 1,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     2, // Would normally scale to 2
		CurrentContainers: 1,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should enforce MinContainers when calculated < min")
}

// =============================================================================
// INTEROPERABILITY TESTS - Verify atoms work together correctly
// =============================================================================

// Interop: Latency trigger + MinContainers + MaxContainers all interact correctly
func TestLatencyAwareScaleFunc_Interop_AllConstraints(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    5,
		MinContainers:    2,
		LatencyThreshold: 100,
	})

	// Case 1: Latency trigger wants to scale from 0 to 1, but min is 2
	sample := &endpointAutoscalerSample{
		TotalRequests:     1,
		CurrentContainers: 0,
		LatencyP95Ms:      200,
	}
	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers, "min floor should override latency scale-up to 1")

	// Case 2: Latency trigger wants to scale from 4 to 5, within bounds
	sample = &endpointAutoscalerSample{
		TotalRequests:     10,
		CurrentContainers: 4,
		LatencyP95Ms:      200,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "should scale to 5 within bounds")

	// Case 3: Latency trigger wants to scale from 5 to 6, but max is 5
	sample = &endpointAutoscalerSample{
		TotalRequests:     20,
		CurrentContainers: 5,
		LatencyP95Ms:      200,
	}
	result = latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 5, result.DesiredContainers, "max ceiling should cap at 5")
}

// Interop: Latency takes precedence over velocity
func TestLatencyAwareScaleFunc_Interop_LatencyPrecedence(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		LatencyThreshold: 100,
		QueueVelocity:    50,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     5,
		CurrentContainers: 2,
		LatencyP95Ms:      200, // Exceeds latency threshold
		QueueVelocity:     100, // Also exceeds velocity threshold
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	// Both triggers fire, but latency is checked first
	assert.Equal(t, 3, result.DesiredContainers, "latency trigger should fire first")
}

// Interop: Queue-depth fallback respects MinContainers
func TestLatencyAwareScaleFunc_Interop_FallbackRespectsMin(t *testing.T) {
	instance := createTestInstance(t, &types.Autoscaler{
		Type:             types.LatencyAutoscaler,
		MaxContainers:    10,
		MinContainers:    3,
		LatencyThreshold: 100,
	})

	sample := &endpointAutoscalerSample{
		TotalRequests:     0, // No requests
		CurrentContainers: 5,
		LatencyP95Ms:      50, // Below threshold, falls back to queue-depth
	}

	result := latencyAwareEndpointScaleFunc(instance, sample)
	assert.True(t, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers, "fallback to queue-depth should still respect MinContainers")
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func createTestInstance(t *testing.T, autoscaler *types.Autoscaler) *endpointInstance {
	t.Helper()

	autoscaledInstance := &abstractions.AutoscaledInstance{
		Ctx: context.Background(),
		Stub: &types.StubWithRelated{
			Stub: types.Stub{
				ExternalId: "test-stub",
			},
		},
		AppConfig: types.AppConfig{},
	}

	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10, // Gateway-level limit
		},
	}

	autoscaledInstance.StubConfig = &types.StubConfigV1{
		Autoscaler: autoscaler,
	}

	instance := &endpointInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	return instance
}

func TestDeploymentScaleFuncWithDefaults(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{
		Ctx: context.Background(),
		Stub: &types.StubWithRelated{
			Stub: types.Stub{
				ExternalId: "test",
			},
		},
		AppConfig: types.AppConfig{},
	}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     1,
		TasksPerContainer: 1,
	}

	instance := &endpointInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	// Check for default scaling up behavior - scale to 1
	sample := &endpointAutoscalerSample{
		TotalRequests: 10,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers)

	// Check for default scaling down behavior - scale to 0
	sample = &endpointAutoscalerSample{
		TotalRequests: 0,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)

	// Check for invalid queue depth
	sample = &endpointAutoscalerSample{
		TotalRequests: -1,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, false, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)
}

func TestDeploymentScaleFuncWithMaxTasksPerContainer(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{
		Ctx: context.Background(),
		Stub: &types.StubWithRelated{
			Stub: types.Stub{
				ExternalId: "test",
			},
		},
		AppConfig: types.AppConfig{},
	}

	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     3,
		TasksPerContainer: 1,
	}

	// Make sure we scale up to max containers
	instance := &endpointInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	sample := &endpointAutoscalerSample{
		TotalRequests: 3,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Ensure we don't exceed max containers
	sample = &endpointAutoscalerSample{
		TotalRequests: 4,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure modulo operator makes sense
	sample = &endpointAutoscalerSample{
		TotalRequests: 11,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure no modulo case works
	sample = &endpointAutoscalerSample{
		TotalRequests: 10,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers)
}
