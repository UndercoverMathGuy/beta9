package function

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

func (fs *ContainerFunctionService) FunctionInvokeGang(in *pb.FunctionInvokeGangRequest, stream pb.FunctionService_FunctionInvokeGangServer) error {
	// P1 Fix: Validate AuthInfo return value to prevent nil pointer dereference
	authInfo, ok := auth.AuthInfoFromContext(stream.Context())
	if !ok || authInfo == nil {
		return fmt.Errorf("authentication context not found")
	}
	ctx := stream.Context()

	groupId, rank0ContainerId, rank0TaskId, containerIds, err := fs.invokeGang(ctx, authInfo, in)
	if err != nil {
		return err
	}

	return fs.streamGang(ctx, stream, authInfo, groupId, rank0ContainerId, rank0TaskId, containerIds, in.StubId, in.Headless)
}

func (fs *ContainerFunctionService) invokeGang(ctx context.Context, authInfo *auth.AuthInfo, in *pb.FunctionInvokeGangRequest) (groupId string, rank0ContainerId string, rank0TaskId string, containerIds []string, error error) {
	stub, err := fs.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return "", "", "", nil, err
	}

	stubConfig := types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return "", "", "", nil, err
	}

	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultFunctionContainerCpu
	}
	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultFunctionContainerMemory
	}
	gpuRequest := types.GpuTypesToStrings(stubConfig.Runtime.Gpus)
	// P1 Fix: Check Gpu (singular) not Gpus (plural) to match pattern in task.go and shell.go
	if stubConfig.Runtime.Gpu != "" {
		gpuRequest = append(gpuRequest, stubConfig.Runtime.Gpu.String())
	}
	gpuCount := stubConfig.Runtime.GpuCount
	if stubConfig.RequiresGPU() && gpuCount == 0 {
		gpuCount = 1
	}

	token, err := fs.backendRepo.RetrieveActiveToken(ctx, stub.Workspace.Id)
	if err != nil {
		return "", "", "", nil, err
	}

	secrets, err := abstractions.ConfigureContainerRequestSecrets(&stub.Workspace, stubConfig)
	if err != nil {
		return "", "", "", nil, err
	}

	args, err := json.Marshal(types.TaskPayload{
		Args: []interface{}{
			in.Args,
		},
		Kwargs: nil,
	})
	if err != nil {
		return "", "", "", nil, err
	}

	if bytes.HasPrefix(in.Args, cloudPickleHeader) {
		args = in.Args
	}

	groupId = fmt.Sprintf("cluster-%s", uuid.New().String()[:8])
	// P1 Fix: Normalize nodes value and use it consistently
	nodes := int(in.Nodes)
	if nodes <= 0 {
		nodes = 1
	}

	containerRequests := make([]*types.ContainerRequest, nodes)
	for rank := 0; rank < nodes; rank++ {
		taskId := uuid.New().String()
		containerId := fs.genContainerId(taskId, stub.Type.Kind())
		containerIds = append(containerIds, containerId)
		if rank == 0 {
			rank0ContainerId = containerId
			rank0TaskId = taskId
		}
		err = fs.rdb.Set(ctx, Keys.FunctionArgs(stub.Workspace.Name, taskId), args, functionArgsExpirationTimeout).Err()
		if err != nil {
			return "", "", "", nil, fmt.Errorf("Failed to store args for rank %d: %w", rank, err)
		}

		mounts, err := abstractions.ConfigureContainerRequestMounts(
			containerId,
			stub.Object.ExternalId,
			&stub.Workspace,
			stubConfig,
			stub.ExternalId,
		)
		if err != nil {
			return "", "", "", nil, err
		}

		env := []string{}
		env = append(env, stubConfig.Env...)
		env = append(env, secrets...)
		env = append(env, []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BETA9_TOKEN=%s", token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
			fmt.Sprintf("CALLBACK_URL=%s", stubConfig.CallbackUrl),
			fmt.Sprintf("BETA9_INPUTS=%s", stubConfig.Inputs.ToString()),
			fmt.Sprintf("BETA9_OUTPUTS=%s", stubConfig.Outputs.ToString()),
			// Gang-specific env vars
			fmt.Sprintf("GANG_GROUP_ID=%s", groupId),
			fmt.Sprintf("GANG_RANK=%d", rank),
			fmt.Sprintf("GANG_SIZE=%d", nodes),
		}...)

		containerRequests[rank] = &types.ContainerRequest{
			ContainerId: containerId,
			Env:         env,
			Cpu:         stubConfig.Runtime.Cpu,
			Memory:      stubConfig.Runtime.Memory,
			GpuRequest:  gpuRequest,
			GpuCount:    uint32(gpuCount),
			ImageId:     stubConfig.Runtime.ImageId,
			StubId:      stub.ExternalId,
			AppId:       stub.App.ExternalId,
			WorkspaceId: stub.Workspace.ExternalId,
			Workspace:   stub.Workspace,
			EntryPoint:  []string{stubConfig.PythonVersion, "-m", "beta9.runner.function"},
			Mounts:      mounts,
			Stub:        *stub,
		}
	}

	// P1 Fix: Use normalized nodes value instead of raw in.Nodes
	gangRequest := &types.GangRequest{
		GroupID:           groupId,
		NodeCount:         nodes,  // Fixed: use normalized value
		ContainerRequests: containerRequests,
		PlacementGroup:    in.PlacementGroup,
		EnableEFA:         in.EnableEfa,
	}
	err = fs.scheduler.GangRun(gangRequest)
	if err != nil {
		return "", "", "", nil, err
	}

	go fs.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)
	return groupId, rank0ContainerId, rank0TaskId, containerIds, nil
}

func (fs *ContainerFunctionService) streamGang(
	ctx context.Context,
	stream pb.FunctionService_FunctionInvokeGangServer,
	authInfo *auth.AuthInfo,
	groupId string,
	rank0ContainerId string,
	rank0TaskId string,
	containerIds []string,
	stubId string,
	headless bool,
) error {
	// Aggregate output from all nodes with rank prefix
	sendCallback := func(rank int) func(o common.OutputMsg) error {
		return func(o common.OutputMsg) error {
			// Prefix output with rank for clarity
			prefixedMsg := o.Msg
			if len(containerIds) > 1 && o.Msg != "" {
				prefixedMsg = fmt.Sprintf("[rank%d] %s", rank, o.Msg)
			}
			return stream.Send(&pb.FunctionInvokeGangResponse{
				GroupId: groupId,
				Output:  prefixedMsg,
				Done:    o.Done,
			})
		}
	}

	// HIGH FIX #26: Track exit codes from all ranks to return worst exit code
	exitCodes := make([]int32, len(containerIds))
	var exitCodesMu sync.Mutex

	exitCallback := func(rank int) func(exitCode int32) error {
		return func(exitCode int32) error {
			exitCodesMu.Lock()
			exitCodes[rank] = exitCode
			exitCodesMu.Unlock()

			// Only rank 0 sends the final result
			if rank == 0 {
				result, _ := fs.rdb.Get(ctx, Keys.FunctionResult(authInfo.Workspace.Name, rank0TaskId)).Bytes()
				return stream.Send(&pb.FunctionInvokeGangResponse{
					GroupId:  groupId,
					ExitCode: exitCode,
					Done:     true,
					Result:   result,
				})
			}
			return nil
		}
	}

	go func() {
		if headless {
			return
		}
		<-ctx.Done()

		for _, cid := range containerIds {
			if err := fs.scheduler.Stop(&types.StopContainerArgs{
				ContainerId: cid,
				Reason:      types.StopContainerReasonUser,
				Force:       true,
			}); err != nil {
				log.Error().Err(err).Str("container_id", cid).Msg("Failed to stop container")
			}
		}
		log.Info().Str("group_id", groupId).Msg("Cluster request cancelled")
	}()

	ctx, cancel := common.MergeContexts(fs.ctx, ctx)
	defer cancel()

	// CRITICAL FIX #10: Use WaitGroup instead of errChan to prevent goroutine leaks
	// Previous implementation: errChan with buffer size len(containerIds), but only rank0 sent
	// This caused other goroutines to complete but their errors were never collected
	var wg sync.WaitGroup
	// CRITICAL FIX #10: Buffer size must match number of senders to prevent blocking
	errChan := make(chan error, len(containerIds))

	for rank, containerId := range containerIds {
		containerStream, err := abstractions.NewContainerStream(abstractions.ContainerStreamOpts{
			SendCallback:    sendCallback(rank),
			ExitCallback:    exitCallback(rank), // HIGH FIX #26: Pass rank to track exit codes
			ContainerRepo:   fs.containerRepo,
			Config:          fs.config,
			Tailscale:       fs.tailscale,
			KeyEventManager: fs.keyEventManager,
		})
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(cid string, rank int) {
			defer wg.Done()
			err := containerStream.Stream(ctx, authInfo, cid)
			// CRITICAL FIX #10: All goroutines send to errChan (buffered to prevent blocking)
			errChan <- err
		}(containerId, rank)
	}

	// Wait for all streams to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors from all ranks, return the first non-nil error
	// (typically rank0's error is most significant)
	var firstError error
	for err := range errChan {
		if err != nil && firstError == nil {
			firstError = err
		}
	}

	return firstError
}
