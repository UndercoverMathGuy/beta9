package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

// P2: Add timeout for Redis operations
const gangBacklogOperationTimeout = 5 * time.Second

// P1 Fix: Remove context storage from struct (Go anti-pattern)
// Contexts should be passed as function parameters
type RequestGangBacklog struct {
	rdb *common.RedisClient
	mu  sync.Mutex
}

func NewRequestGangBacklog(rdb *common.RedisClient) *RequestGangBacklog {
	return &RequestGangBacklog{rdb: rdb}
}

// Pushes a new container request into the sorted set
// P1 Fix: Accept context as first parameter
func (rb *RequestGangBacklog) GangPush(ctx context.Context, request *types.GangRequest) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	jsonData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Use context with timeout
	ctx, cancel := context.WithTimeout(ctx, gangBacklogOperationTimeout)
	defer cancel()

	// Use the timestamp as the score for sorting
	timestamp := float64(request.Timestamp.UnixNano())
	return rb.rdb.ZAdd(ctx, common.RedisKeys.SchedulerGangContainerRequests(), redis.Z{Score: timestamp, Member: jsonData}).Err()
}

// GangPopIfExists atomically pops and returns the oldest request if queue is non-empty
// Uses ZPOPMIN which is atomic - if the queue is empty, returns nil without error
// P1 Fix: Accept context as first parameter
func (rb *RequestGangBacklog) GangPopIfExists(ctx context.Context) (*types.GangRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Use context with timeout
	ctx, cancel := context.WithTimeout(ctx, gangBacklogOperationTimeout)
	defer cancel()

	// ZPOPMIN atomically pops the element with lowest score (oldest timestamp)
	// If the set is empty, it returns an empty slice - no race condition possible
	result, err := rb.rdb.ZPopMin(ctx, common.RedisKeys.SchedulerGangContainerRequests(), 1).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		// Queue was empty - this is not an error, just no work available
		return nil, nil
	}

	var poppedItem types.GangRequest
	// P1 Fix: Safe type assertion handling both string and []byte
	var memberBytes []byte
	switch v := result[0].Member.(type) {
	case string:
		memberBytes = []byte(v)
	case []byte:
		memberBytes = v
	default:
		return nil, fmt.Errorf("unexpected Redis member type: %T", result[0].Member)
	}

	err = json.Unmarshal(memberBytes, &poppedItem)
	if err != nil {
		return nil, err
	}

	return &poppedItem, nil
}

// Pops the oldest container request from the sorted set
// Deprecated: Use GangPopIfExists for race-free operation
// P1 Fix: Accept context as first parameter
func (rb *RequestGangBacklog) GangPop(ctx context.Context) (*types.GangRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Use context with timeout
	ctx, cancel := context.WithTimeout(ctx, gangBacklogOperationTimeout)
	defer cancel()

	result, err := rb.rdb.ZPopMin(ctx, common.RedisKeys.SchedulerGangContainerRequests(), 1).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	var poppedItem types.GangRequest
	// P1 Fix: Safe type assertion
	var memberBytes []byte
	switch v := result[0].Member.(type) {
	case string:
		memberBytes = []byte(v)
	case []byte:
		memberBytes = v
	default:
		return nil, fmt.Errorf("unexpected Redis member type: %T", result[0].Member)
	}

	err = json.Unmarshal(memberBytes, &poppedItem)
	if err != nil {
		return nil, err
	}

	return &poppedItem, nil
}

// Gets the length of the sorted set
// Note: For checking if work exists, prefer GangPopIfExists to avoid TOCTOU race
// P2 Fix: Return error to properly surface Redis errors
func (rb *RequestGangBacklog) GangLen(ctx context.Context) (int64, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Use context with timeout
	ctx, cancel := context.WithTimeout(ctx, gangBacklogOperationTimeout)
	defer cancel()

	// P2 Fix: Use .Result() instead of .Val() to get error handling
	count, err := rb.rdb.ZCard(ctx, common.RedisKeys.SchedulerGangContainerRequests()).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get gang backlog length: %w", err)
	}
	return count, nil
}
