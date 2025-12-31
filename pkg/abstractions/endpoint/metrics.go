package endpoint

import (
	"context"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type LatencyMetrics struct {
	P50Ms         float64
	P95Ms         float64
	QueueVelocity float64
}

// PercentileValue calculates the nth percentile using linear interpolation.
// MEDIUM FIX #20: Added bounds checking for percentile value
func PercentileValue(n float64, values *[]int) float64 {
	length := len(*values)
	if length == 0 {
		return 0
	}
	if length == 1 {
		return float64((*values)[0])
	}
	// Ensure n is within valid range [0, 100]
	if n < 0 {
		n = 0
	}
	if n > 100 {
		n = 100
	}
	idx := n * (float64(length) - 1) / 100
	lower := int(idx)
	upper := lower + 1
	if upper >= length {
		return float64((*values)[length-1])
	}
	fraction := idx - float64(lower)
	return float64((*values)[lower])*(1-fraction) + float64((*values)[upper])*fraction
}

func (rb *RequestBuffer) GetLatencyMetrics(ctx context.Context, workspaceName string, stubId string) (LatencyMetrics, error) {
	metrics := LatencyMetrics{}

	latencyKey := Keys.endpointLatencyWindow(workspaceName, stubId)
	rawLatencies, err := rb.rdb.LRange(ctx, latencyKey, 0, -1)
	if err != nil {
		return metrics, err
	}
	if len(rawLatencies) >= 1 {
		values := []int{}
		for _, value := range rawLatencies {
			parsedValue, err := strconv.Atoi(value)
			if err != nil {
				return metrics, err
			}
			values = append(values, parsedValue)
		}
		sort.Ints(values)
		metrics.P50Ms = PercentileValue(50, &values)
		metrics.P95Ms = PercentileValue(95, &values)
	}

	velocityKey := Keys.endpointQueueVelocity(workspaceName, stubId)
	velocity, err := rb.rdb.Get(ctx, velocityKey).Float64()
	if err != nil {
		if err == redis.Nil {
			return metrics, nil
		}
		return metrics, err
	}
	metrics.QueueVelocity = velocity

	return metrics, nil
}
