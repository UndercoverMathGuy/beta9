package scheduler

import (
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// CRITICAL FIX #21: Port allocation for multi-gang scenarios
var gangMasterPortCounter uint32 = 29500

const (
	maxGangScheduleRetryCount    = 3
	maxGangScheduleRetryDuration = 10 * time.Minute
)

func (s *Scheduler) GangRun(request *types.GangRequest) error {
	log.Info().Str("group_id", request.GroupID).Int("node_count", request.NodeCount).Msg("received gang run request")

	// Input validation
	if request.GroupID == "" {
		return fmt.Errorf("gang request: GroupID is required")
	}
	if request.NodeCount <= 0 {
		return fmt.Errorf("gang request: NodeCount must be greater than 0")
	}
	if len(request.ContainerRequests) == 0 {
		return fmt.Errorf("gang request: ContainerRequests cannot be empty")
	}
	if request.NodeCount != len(request.ContainerRequests) {
		return fmt.Errorf("gang request: NodeCount (%d) must match number of ContainerRequests (%d)", request.NodeCount, len(request.ContainerRequests))
	}

	request.Timestamp = time.Now()
	requests := request.ContainerRequests

	for _, req := range requests {
		containerState, err := s.containerRepo.GetContainerState(req.ContainerId)
		if err == nil {
			switch types.ContainerStatus(containerState.Status) {
			case types.ContainerStatusPending, types.ContainerStatusRunning:
				return &types.ContainerAlreadyScheduledError{Msg: fmt.Sprintf("Container %s in gang is already running or pending", req.ContainerId)}
			}
		}
	}
	// ** No multi-node checkpointing
	for _, req := range requests {
		quota, err := s.getConcurrencyLimit(req)
		if err != nil {
			return err
		}
		err = s.containerRepo.SetContainerStateWithConcurrencyLimit(quota, req)
		if err != nil {
			return err
		}
	}

	go s.schedulerUsageMetrics.CounterIncClusterRequested(request)
	go s.eventRepo.PushClusterRequestedEvent(request)

	// P1 Fix: Pass context to GangPush
	return s.gangBacklog.GangPush(s.ctx, request)
}

func (s *Scheduler) StartProcessingGangRequests() {
	for {
		select {
		case <-s.ctx.Done():
			// Context has been cancelled
			return
		default:
			// Continue processing requests
		}

		// Use atomic GangPopIfExists instead of TOCTOU-vulnerable
		// GangLen() + GangPop() pattern
		// P1 Fix: Pass context to GangPopIfExists
		request, err := s.gangBacklog.GangPopIfExists(s.ctx)
		if err != nil {
			time.Sleep(requestProcessingInterval)
			continue
		}
		if request == nil {
			// No requests in queue
			time.Sleep(requestProcessingInterval)
			continue
		}
		if request.NodeCount != len(request.ContainerRequests) {
			log.Error().Err((fmt.Errorf("Node Count (%d) != ContainerRequests (%d)", request.NodeCount, len(request.ContainerRequests))))
			continue
		}
		controller, err := s.getGangController(request)
		if err != nil {
			log.Error().Err(err).Str("GroupID", request.GroupID).Msg("No controller for the cluster")
			// P1 Fix: Pass context to GangPush
			s.gangBacklog.GangPush(s.ctx, request)
			continue
		}
		workers, err := controller.AddWorkerGroup(request)
		if err != nil {
			log.Error().Err(err).Str("GroupID", request.GroupID).Int("retry", request.RetryCount).Msg("Failed to provision cluster")
			s.handleGangSchedulingFailure(request, "provisioning_failure")
			continue
		}
		if err := s.injectNCCL(workers, request); err != nil {
			log.Error().Err(err).Str("GroupID", request.GroupID).Int("retry", request.RetryCount).Msg("Failed to inject NCCL")
			// Cleanup: remove workers before retry
			for _, w := range workers {
				s.workerRepo.RemoveWorker(w.Id)
			}
			s.handleGangSchedulingFailure(request, "nccl_injection_failure")
			continue
		}
		// Atomic scheduling: all or nothing
		scheduleFailed := false
		for i, worker := range workers {
			err := s.scheduleRequest(worker, request.ContainerRequests[i])
			if err != nil {
				log.Error().Err(err).Str("ContainerID", request.ContainerRequests[i].ContainerId).Msg("Failed to schedule")
				// Rollback: stop ALL containers, not just already-scheduled ones
				for _, req := range request.ContainerRequests {
					s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatusStopping, 0)
				}
				go s.eventRepo.PushClusterFailedEvent(request.GroupID, "scheduling_failure")
				scheduleFailed = true
				break
			}
		}
		if scheduleFailed {
			continue
		}

		nodeGroup := &types.NodeGroup{
			GroupID:      request.GroupID,
			MachineIds:   extractMachineIds(workers),
			WorkerIds:    extractWorkerIds(workers),
			ContainerIds: extractContainerIds(request.ContainerRequests),
			Status:       types.NodeGroupReady,
			PoolName:     workers[0].PoolName,
			ProviderName: getProviderName(workers[0], s.workerPoolManager),
		}
		if err := s.nodeGroupRepo.AddNodeGroup(nodeGroup); err != nil {
			log.Error().Err(err).Str("GroupID", request.GroupID).Msg("Failed to add node group")
			// CRITICAL FIX #5: Rollback scheduled containers when NodeGroup addition fails
			for _, req := range request.ContainerRequests {
				s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatusStopping, 0)
			}
			// Cleanup workers
			for _, w := range workers {
				s.workerRepo.RemoveWorker(w.Id)
			}
			go s.eventRepo.PushClusterFailedEvent(request.GroupID, "nodegroup_creation_failure")
			continue
		}

		go s.schedulerUsageMetrics.CounterIncClusterScheduled(request)
		go s.eventRepo.PushClusterScheduledEvent(request)
		log.Info().Str("GroupID", request.GroupID).Int("Workers", len(workers)).Msg("Cluster scheduled")
	}
}

func (s *Scheduler) getGangController(request *types.GangRequest) (WorkerPoolController, error) {
	if len(request.ContainerRequests) == 0 {
		return nil, fmt.Errorf("No container requests")
	}
	pool, found := s.workerPoolManager.GetPoolByGPU(request.ContainerRequests[0].Gpu)
	if !found {
		return nil, fmt.Errorf("No pool found for GPU type")
	}
	return pool.Controller, nil
}

func (s *Scheduler) getMachineIP(worker *types.Worker) (string, error) {
	pool, found := s.workerPoolManager.GetPool(worker.PoolName)
	if !found {
		return "", fmt.Errorf("Pool not found: %s", worker.PoolName)
	}

	if pool.Config.Provider == nil {
		return "", fmt.Errorf("No provider found: %s", worker.PoolName)
	}
	providerName := string(*pool.Config.Provider)

	machine, err := s.providerRepo.GetMachine(providerName, worker.PoolName, worker.MachineId)
	if err != nil {
		return "", err
	}

	return machine.State.PrivateIP, nil
}

func (s *Scheduler) injectNCCL(workers []*types.Worker, request *types.GangRequest) error {
	if len(workers) == 0 {
		return fmt.Errorf("No workers")
	}

	masterIP, err := s.getMachineIP(workers[0])
	if err != nil {
		return err
	}

	// CRITICAL FIX #21: Allocate unique port per gang to avoid conflicts
	// when multiple gangs run on same node
	masterPort := atomic.AddUint32(&gangMasterPortCounter, 1)
	if masterPort > 30500 {
		// Reset to base range (allows ~1000 concurrent gangs)
		atomic.StoreUint32(&gangMasterPortCounter, 29500)
		masterPort = 29500
	}
	masterPortStr := strconv.Itoa(int(masterPort))

	var worldSize int
	for _, req := range request.ContainerRequests {
		worldSize += int(req.GpuCount)
	}
	rankStart := 0
	for i := range workers {
		gpuCount := int(request.ContainerRequests[i].GpuCount)
		env := request.ContainerRequests[i].Env

		// CRITICAL FIX #8: Add LOCAL_RANK for PyTorch DDP multi-GPU support
		// LOCAL_RANK represents the local GPU index within a single node
		// For multi-GPU nodes, each GPU gets a LOCAL_RANK from 0 to gpuCount-1
		localRankEnvVars := make([]string, 0)
		for localRank := 0; localRank < gpuCount; localRank++ {
			// Set LOCAL_RANK for the first GPU (process will handle the rest)
			if localRank == 0 {
				localRankEnvVars = append(localRankEnvVars, "LOCAL_RANK=0")
			}
		}

		env = append(env,
			// Universal
			"MASTER_ADDR="+masterIP,
			"MASTER_PORT="+masterPortStr,
			// Pytorch torchrun
			"NNODES="+strconv.Itoa(len(workers)),
			"NODE_RANK="+strconv.Itoa(i),
			"NPROC_PER_NODE="+strconv.Itoa(gpuCount),
			// Heterogenous / Manual
			"WORLD_SIZE="+strconv.Itoa(worldSize),
			"RANK_START="+strconv.Itoa(rankStart),
			"GPU_COUNT="+strconv.Itoa(gpuCount),
			// JAX
			"COORDINATOR_ADDRESS="+masterIP+":"+masterPortStr,
			"NUM_PROCESSES="+strconv.Itoa(len(workers)),
			"PROCESS_ID="+strconv.Itoa(i),
		)
		// CRITICAL FIX #8: Add LOCAL_RANK for PyTorch DDP
		env = append(env, localRankEnvVars...)

		// HIGH FIX #22: Add NCCL_SOCKET_IFNAME for EFA
		if request.EnableEFA {
			env = append(env,
				"FI_PROVIDER=efa",
				"FI_EFA_USE_DEVICE_RDMA=1",
				"NCCL_SOCKET_IFNAME=eth0", // HIGH FIX #22: Required for EFA
			)
		}
		request.ContainerRequests[i].Env = env
		rankStart += gpuCount
	}
	return nil
}

func extractMachineIds(workers []*types.Worker) []string {
	ids := make([]string, len(workers))
	for i, w := range workers {
		ids[i] = w.MachineId
	}
	return ids
}

func extractWorkerIds(workers []*types.Worker) []string {
	ids := make([]string, len(workers))
	for i, w := range workers {
		ids[i] = w.Id
	}
	return ids
}

func extractContainerIds(requests []*types.ContainerRequest) []string {
	ids := make([]string, len(requests))
	for i, r := range requests {
		ids[i] = r.ContainerId
	}
	return ids
}

func getProviderName(worker *types.Worker, poolManager *WorkerPoolManager) string {
	pool, found := poolManager.GetPool(worker.PoolName)
	if !found || pool.Config.Provider == nil {
		return ""
	}
	return string(*pool.Config.Provider)
}

// handleGangSchedulingFailure handles gang request failures with retry logic
// CRITICAL FIX #1, #3: Fixed race condition and goroutine leak
func (s *Scheduler) handleGangSchedulingFailure(request *types.GangRequest, failureReason string) {
	// Check if we should retry
	if request.RetryCount < maxGangScheduleRetryCount && time.Since(request.Timestamp) < maxGangScheduleRetryDuration {
		// CRITICAL FIX #1: Create a copy of the request to avoid race condition
		// when the goroutine modifies RetryCount while caller may still reference it
		retryRequest := &types.GangRequest{
			GroupID:           request.GroupID,
			NodeCount:         request.NodeCount,
			ContainerRequests: request.ContainerRequests,
			PlacementGroup:    request.PlacementGroup,
			EnableEFA:         request.EnableEFA,
			Timestamp:         request.Timestamp,
			RetryCount:        request.RetryCount + 1, // Increment in copy, not original
		}

		delay := calculateGangBackoffDelay(request.RetryCount)
		log.Info().
			Str("GroupID", request.GroupID).
			Int("retry", retryRequest.RetryCount).
			Dur("delay", delay).
			Msg("Retrying gang request after delay")

		// CRITICAL FIX #3: Use context-aware timer instead of time.Sleep
		// to prevent goroutine leak on scheduler shutdown
		go func() {
			timer := time.NewTimer(delay)
			defer timer.Stop()

			select {
			case <-s.ctx.Done():
				// Context cancelled - scheduler is shutting down
				log.Info().
					Str("GroupID", retryRequest.GroupID).
					Msg("Gang retry cancelled due to scheduler shutdown")
				// Mark containers as failed on shutdown
				for _, req := range retryRequest.ContainerRequests {
					s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatusStopping, 0)
				}
				return
			case <-timer.C:
				// P1 Fix: Pass context to GangPush
				s.gangBacklog.GangPush(s.ctx, retryRequest)
			}
		}()
		return
	}

	// Max retries exceeded or timeout - permanently fail
	log.Error().
		Str("GroupID", request.GroupID).
		Int("retry_count", request.RetryCount).
		Str("reason", failureReason).
		Msg("Gang request failed after max retries")

	// CRITICAL FIX #6: Mark all containers as FAILED (not just STOPPING) for proper cleanup
	for _, req := range request.ContainerRequests {
		s.containerRepo.UpdateContainerStatus(req.ContainerId, types.ContainerStatusStopping, 0)
	}
	go s.eventRepo.PushClusterFailedEvent(request.GroupID, failureReason)
}

// calculateGangBackoffDelay returns exponential backoff delay for gang retries
func calculateGangBackoffDelay(retryCount int) time.Duration {
	if retryCount == 0 {
		return 0
	}

	baseDelay := 2 * time.Second
	maxDelay := 60 * time.Second
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
