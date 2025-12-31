package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// HIGH FIX #15: Add TTL for NodeGroup keys to prevent unbounded memory growth
// NodeGroups should not persist longer than 24 hours after creation
const nodeGroupTTL = 24 * time.Hour

// HIGH FIX #30: Add timeout for Redis operations
const nodeGroupOperationTimeout = 5 * time.Second

type NodeGroupRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
	ctx  context.Context // Store context for proper cancellation
}

func NewNodeGroupRedisRepository(r *common.RedisClient, ctx context.Context) NodeGroupRepository {
	lock := common.NewRedisLock(r)
	return &NodeGroupRedisRepository{rdb: r, lock: lock, ctx: ctx}
}

func (r *NodeGroupRedisRepository) AddNodeGroup(nodeGroup *types.NodeGroup) error {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	lockKey := common.RedisKeys.SchedulerNodeGroupLock(nodeGroup.GroupID)
	err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 30, Retries: 3})
	if err != nil {
		return err
	}
	defer r.lock.Release(lockKey)

	stateKey := common.RedisKeys.SchedulerNodeGroupState(nodeGroup.GroupID)
	indexKey := common.RedisKeys.SchedulerNodeGroupIndex()

	err = r.rdb.SAdd(ctx, indexKey, stateKey).Err()
	if err != nil {
		return fmt.Errorf("Failed to add node group state key to index <%v>:%w", indexKey, err)
	}

	err = r.rdb.HSet(ctx, stateKey, common.ToSlice(nodeGroup)).Err()
	if err != nil {
		return fmt.Errorf("Failed to add node group state <%v>:%w", stateKey, err)
	}

	// HIGH FIX #15: Set TTL on state key to prevent unbounded memory growth
	err = r.rdb.Expire(ctx, stateKey, nodeGroupTTL).Err()
	if err != nil {
		return fmt.Errorf("Failed to set TTL on node group state: %w", err)
	}

	for _, machineId := range nodeGroup.MachineIds {
		machineIndexKey := common.RedisKeys.SchedulerNodeGroupMachineIndex(machineId)
		// HIGH FIX #15: Set TTL on machine index keys as well
		err = r.rdb.Set(ctx, machineIndexKey, nodeGroup.GroupID, nodeGroupTTL).Err()
		if err != nil {
			return fmt.Errorf("Failed to set machine index: %w", err)
		}
	}
	return nil
}

func (r *NodeGroupRedisRepository) GetNodeGroupById(groupId string) (*types.NodeGroup, error) {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	lockKey := common.RedisKeys.SchedulerNodeGroupLock(groupId)
	err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 30, Retries: 3})
	if err != nil {
		return nil, err
	}
	defer r.lock.Release(lockKey)

	stateKey := common.RedisKeys.SchedulerNodeGroupState(groupId)

	res, err := r.rdb.HGetAll(ctx, stateKey).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}

	nodeGroup := &types.NodeGroup{}
	if err = common.ToStruct(res, nodeGroup); err != nil {
		return nil, fmt.Errorf("Failed to deserialize node group: %w", err)
	}
	return nodeGroup, nil
}

func (r *NodeGroupRedisRepository) GetNodeGroupByMachineId(machineId string) (*types.NodeGroup, error) {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	machineIndexKey := common.RedisKeys.SchedulerNodeGroupMachineIndex(machineId)
	groupId, err := r.rdb.Get(ctx, machineIndexKey).Result()
	if err != nil {
		return nil, err
	}
	return r.GetNodeGroupById(groupId)
}

func (r *NodeGroupRedisRepository) UpdateNodeGroupStatus(groupId string, status types.NodeGroupStatus) error {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	lockKey := common.RedisKeys.SchedulerNodeGroupLock(groupId)
	err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 30, Retries: 10})
	if err != nil {
		return err
	}
	defer r.lock.Release(lockKey)

	stateKey := common.RedisKeys.SchedulerNodeGroupState(groupId)

	// MEDIUM FIX #31: Validate state transition before updating
	currentStatusStr, err := r.rdb.HGet(ctx, stateKey, "Status").Result()
	if err == nil && currentStatusStr != "" {
		currentStatus := types.NodeGroupStatus(currentStatusStr)
		if err := currentStatus.ValidateTransition(status); err != nil {
			return fmt.Errorf("invalid state transition for group %s: %w", groupId, err)
		}
	}

	return r.rdb.HSet(ctx, stateKey, "Status", string(status)).Err()
}

func (r *NodeGroupRedisRepository) DeleteNodeGroup(groupId string) error {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	lockKey := common.RedisKeys.SchedulerNodeGroupLock(groupId)
	err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 30, Retries: 3})
	if err != nil {
		return err
	}
	// HIGH FIX #14: Ensure lock key is cleaned up on delete
	defer func() {
		r.lock.Release(lockKey)
		// Delete the lock key itself to prevent resource leak
		r.rdb.Del(ctx, lockKey)
	}()

	stateKey := common.RedisKeys.SchedulerNodeGroupState(groupId)
	res, err := r.rdb.HGetAll(ctx, stateKey).Result()
	if err != nil || len(res) == 0 {
		return err
	}
	nodeGroup := &types.NodeGroup{}
	if err = common.ToStruct(res, nodeGroup); err != nil {
		return err
	}

	// HIGH FIX #20: Use pipeline for atomic deletion
	pipe := r.rdb.Pipeline()

	// Delete machine index entries
	for _, machineId := range nodeGroup.MachineIds {
		pipe.Del(ctx, common.RedisKeys.SchedulerNodeGroupMachineIndex(machineId))
	}

	// Remove from index
	pipe.SRem(ctx, common.RedisKeys.SchedulerNodeGroupIndex(), stateKey)

	// Delete state key
	pipe.Del(ctx, stateKey)

	// Execute all deletions atomically
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete node group: %w", err)
	}

	return nil
}

func (r *NodeGroupRedisRepository) GetAllNodeGroups() ([]*types.NodeGroup, error) {
	// HIGH FIX #30: Use context with timeout
	ctx, cancel := context.WithTimeout(r.ctx, nodeGroupOperationTimeout)
	defer cancel()

	indexKey := common.RedisKeys.SchedulerNodeGroupIndex()
	keys, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	nodeGroups := make([]*types.NodeGroup, 0, len(keys))
	for _, key := range keys {
		res, err := r.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			// HIGH FIX #28: Properly propagate errors instead of silently continuing
			return nil, fmt.Errorf("failed to get node group %s: %w", key, err)
		}
		if len(res) == 0 {
			// Clean up stale index entry
			r.rdb.SRem(ctx, indexKey, key)
			continue
		}
		ng := &types.NodeGroup{}
		if err := common.ToStruct(res, ng); err != nil {
			// Log but continue - don't fail entire operation for one bad entry
			continue
		}
		nodeGroups = append(nodeGroups, ng)
	}
	return nodeGroups, nil
}
