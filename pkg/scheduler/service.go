package scheduler

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type SchedulerService struct {
	pb.UnimplementedSchedulerServer
	Scheduler *Scheduler
}

func NewSchedulerService(scheduler *Scheduler) (*SchedulerService, error) {
	go scheduler.StartProcessingRequests() // Start processing ContainerRequests
	go scheduler.StartProcessingGangRequests()
	gangHealthMonitor := NewGangHealthMonitor(
		scheduler.ctx,
		scheduler.nodeGroupRepo,
		scheduler.providerRepo,
		scheduler.containerRepo,
		scheduler.eventRepo,
	)
	go gangHealthMonitor.Start()
	return &SchedulerService{
		Scheduler: scheduler,
	}, nil
}

// Get Scheduler version
func (wbs *SchedulerService) GetVersion(ctx context.Context, in *pb.VersionRequest) (*pb.VersionResponse, error) {
	return &pb.VersionResponse{Version: SchedulerConfig.Version}, nil
}

// Run a container
func (wbs *SchedulerService) RunContainer(ctx context.Context, in *pb.RunContainerRequest) (*pb.RunContainerResponse, error) {
	cpuRequest, err := ParseCPU(in.Cpu)
	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	memoryRequest, err := ParseMemory(in.Memory)
	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	err = wbs.Scheduler.Run(&types.ContainerRequest{
		ContainerId: in.ContainerId,
		EntryPoint:  in.EntryPoint,
		Env:         in.Env,
		Cpu:         cpuRequest,
		Memory:      memoryRequest,
		Gpu:         in.Gpu,
		ImageId:     in.ImageId,
		RetryCount:  0,
	})

	if err != nil {
		return &pb.RunContainerResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.RunContainerResponse{
		Success: true,
		Error:   "",
	}, nil
}

func (wbs *SchedulerService) RunGang(ctx context.Context, in *pb.RunGangRequest) (*pb.RunGangResponse, error) {
	containerRequests := make([]*types.ContainerRequest, 0, len(in.ContainerRequests))

	for _, cr := range in.ContainerRequests {
		cpuRequest, err := ParseCPU(cr.Cpu)
		if err != nil {
			return &pb.RunGangResponse{Success: false, Error: err.Error()}, nil
		}
		memoryRequest, err := ParseMemory(cr.Memory)
		if err != nil {
			return &pb.RunGangResponse{Success: false, Error: err.Error()}, nil
		}

		containerRequests = append(containerRequests, &types.ContainerRequest{
			ContainerId: cr.ContainerId,
			EntryPoint:  cr.EntryPoint,
			Env:         cr.Env,
			Cpu:         cpuRequest,
			Memory:      memoryRequest,
			Gpu:         cr.Gpu,
			GpuCount:    cr.GpuCount,
			ImageId:     cr.ImageId,
			StubId:      cr.StubId,
			WorkspaceId: cr.WorkspaceId,
		})
	}
	gangRequest := &types.GangRequest{
		GroupID:           in.GroupId,
		NodeCount:         int(in.NodeCount),
		ContainerRequests: containerRequests,
		PlacementGroup:    in.PlacementGroup,
		EnableEFA:         in.EnableEfa,
	}
	err := wbs.Scheduler.GangRun(gangRequest)
	if err != nil {
		return &pb.RunGangResponse{
			Success: false,
			Error:   err.Error(),
			GroupId: in.GroupId,
		}, nil
	}
	return &pb.RunGangResponse{
		Success: true,
		GroupId: in.GroupId,
	}, nil
}
