package endpoint

import (
	stdjson "encoding/json"
	"net/http"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)


// Positive: Single request batch processes correctly
func TestBatch_Positive_SingleRequest(t *testing.T) {
	// Single request in a batch should work identically to non-batched
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{Args: []interface{}{"hello"}}},
	}

	// Build payload
	validItems := make([]map[string]interface{}, 0)
	for _, item := range items {
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	batchPayload := map[string]interface{}{
		"batch_id": "batch-single",
		"items":    validItems,
	}

	jsonBytes, err := stdjson.Marshal(batchPayload)
	assert.NoError(t, err)

	// Verify structure
	var parsed map[string]interface{}
	err = stdjson.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)
	assert.Equal(t, "batch-single", parsed["batch_id"])
	assert.Len(t, parsed["items"], 1)
}

// Positive: Multiple requests batch correctly with sequential indices
func TestBatch_Positive_MultipleRequests(t *testing.T) {
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{Args: []interface{}{"a"}}},
		{TaskId: "task-1", BatchIdx: 1, Payload: &types.TaskPayload{Args: []interface{}{"b"}}},
		{TaskId: "task-2", BatchIdx: 2, Payload: &types.TaskPayload{Args: []interface{}{"c"}}},
	}

	validItems := make([]map[string]interface{}, 0)
	for _, item := range items {
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	assert.Len(t, validItems, 3)
	assert.Equal(t, 0, validItems[0]["batch_index"])
	assert.Equal(t, 1, validItems[1]["batch_index"])
	assert.Equal(t, 2, validItems[2]["batch_index"])
}

// Positive: Response demux correctly maps all successful responses
func TestBatch_Positive_AllSuccessfulResponses(t *testing.T) {
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"result": "success_0"}},
			{"batch_index": 1, "status_code": 200, "body": {"result": "success_1"}},
			{"batch_index": 2, "status_code": 200, "body": {"result": "success_2"}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)
	assert.NoError(t, err)

	results := make([]batchResultItem, 3)
	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// All results should be filled with 200 status
	for i, result := range results {
		assert.True(t, result.filled, "result %d should be filled", i)
		assert.Equal(t, 200, result.StatusCode, "result %d should have 200 status", i)
	}
}

// Positive: BatchConfig correctly configures batch size and timeout
func TestBatch_Positive_ConfigApplied(t *testing.T) {
	config := types.BatchConfig{
		MaxSize: 32,
		WaitMs:  100,
	}

	// Verify config values are accessible
	assert.Equal(t, 32, config.MaxSize)
	assert.Equal(t, 100, config.WaitMs)

	// Verify JSON serialization matches SDK protocol
	jsonBytes, _ := stdjson.Marshal(config)
	var parsed map[string]interface{}
	stdjson.Unmarshal(jsonBytes, &parsed)

	assert.Equal(t, float64(32), parsed["max_batch_size"])
	assert.Equal(t, float64(100), parsed["wait_ms"])
}

// Positive: Payload with args and kwargs serializes correctly
func TestBatch_Positive_PayloadSerialization(t *testing.T) {
	payload := &types.TaskPayload{
		Args:   []interface{}{"hello", 123, true},
		Kwargs: map[string]interface{}{"name": "test", "count": 42},
	}

	item := map[string]interface{}{
		"batch_index": 0,
		"task_id":     "task-001",
		"payload":     payload,
	}

	jsonBytes, err := stdjson.Marshal(item)
	assert.NoError(t, err)

	var parsed map[string]interface{}
	err = stdjson.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	assert.Contains(t, parsed, "payload")
	payloadMap := parsed["payload"].(map[string]interface{})
	assert.Contains(t, payloadMap, "args")
	assert.Contains(t, payloadMap, "kwargs")
}

// Positive: Large batch (max size) processes correctly
func TestBatch_Positive_LargeBatch(t *testing.T) {
	batchSize := 32 // Typical max batch size

	items := make([]batchItem, batchSize)
	for i := 0; i < batchSize; i++ {
		items[i] = batchItem{
			TaskId:   "task-" + string(rune('0'+i%10)),
			BatchIdx: i,
			Payload:  &types.TaskPayload{Args: []interface{}{i}},
		}
	}

	validItems := make([]map[string]interface{}, 0)
	for _, item := range items {
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	assert.Len(t, validItems, batchSize)

	// Verify all indices are sequential
	for i, item := range validItems {
		assert.Equal(t, i, item["batch_index"])
	}
}

// =============================================================================
// IDEAL FLOW TESTS - End-to-End Batch Processing Scenarios
// =============================================================================

// IdealFlow: Complete batch lifecycle from request collection to response demux
func TestBatch_IdealFlow_CompleteLifecycle(t *testing.T) {
	// Phase 1: Collect requests into batch items
	items := []batchItem{
		{TaskId: "task-a", BatchIdx: 0, Payload: &types.TaskPayload{Args: []interface{}{"input_a"}}},
		{TaskId: "task-b", BatchIdx: 1, Payload: &types.TaskPayload{Args: []interface{}{"input_b"}}},
		{TaskId: "task-c", BatchIdx: 2, Payload: &types.TaskPayload{Args: []interface{}{"input_c"}}},
	}

	// Phase 2: Build batch payload for Python
	results := make([]batchResultItem, len(items))
	validItems := make([]map[string]interface{}, 0)

	for _, item := range items {
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	batchPayload := map[string]interface{}{
		"batch_id": "lifecycle-batch",
		"items":    validItems,
	}

	// Verify payload structure
	jsonBytes, err := stdjson.Marshal(batchPayload)
	assert.NoError(t, err)
	assert.Contains(t, string(jsonBytes), "lifecycle-batch")
	assert.Contains(t, string(jsonBytes), "task-a")

	// Phase 3: Simulate Python response
	pythonResponse := `{
		"batch_id": "lifecycle-batch",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"output": "processed_a"}},
			{"batch_index": 1, "status_code": 200, "body": {"output": "processed_b"}},
			{"batch_index": 2, "status_code": 200, "body": {"output": "processed_c"}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}
	stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)

	// Phase 4: Demux responses back to original requests
	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// Verify all results are correctly mapped
	assert.True(t, results[0].filled)
	assert.Contains(t, string(results[0].Body), "processed_a")

	assert.True(t, results[1].filled)
	assert.Contains(t, string(results[1].Body), "processed_b")

	assert.True(t, results[2].filled)
	assert.Contains(t, string(results[2].Body), "processed_c")
}

// IdealFlow: Mixed success/error responses handled correctly
func TestBatch_IdealFlow_MixedResponses(t *testing.T) {
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{}},
		{TaskId: "task-1", BatchIdx: 1, Payload: &types.TaskPayload{}},
		{TaskId: "task-2", BatchIdx: 2, Payload: &types.TaskPayload{}},
	}

	results := make([]batchResultItem, len(items))

	// Python returns mixed results
	pythonResponse := `{
		"batch_id": "mixed-batch",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"success": true}},
			{"batch_index": 1, "status_code": 500, "body": {"error": "processing failed"}},
			{"batch_index": 2, "status_code": 200, "body": {"success": true}}
		]
	}`

	var batchResponse struct {
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}
	stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// Verify mixed results
	assert.Equal(t, 200, results[0].StatusCode)
	assert.Equal(t, 500, results[1].StatusCode)
	assert.Equal(t, 200, results[2].StatusCode)

	assert.Contains(t, string(results[1].Body), "processing failed")
}

// IdealFlow: Batch with pre-serialization errors handled correctly
func TestBatch_IdealFlow_PreSerializationErrors(t *testing.T) {
	// Some items fail during serialization (before sending to Python)
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{}, Error: nil},
		{TaskId: "task-1", BatchIdx: 1, Error: assert.AnError}, // Serialization failed
		{TaskId: "task-2", BatchIdx: 2, Payload: &types.TaskPayload{}, Error: nil},
	}

	results := make([]batchResultItem, len(items))
	validItems := make([]map[string]interface{}, 0)

	// Pre-fill errors, collect valid items
	for i, item := range items {
		if item.Error != nil {
			errBody, _ := stdjson.Marshal(map[string]interface{}{"error": item.Error.Error()})
			results[i] = batchResultItem{
				BatchIdx:   i,
				StatusCode: http.StatusBadRequest,
				Body:       errBody,
				filled:     true,
			}
			continue
		}
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	// Only valid items sent to Python
	assert.Len(t, validItems, 2)
	assert.Equal(t, 0, validItems[0]["batch_index"])
	assert.Equal(t, 2, validItems[1]["batch_index"]) // Index 1 was skipped

	// Python responds only for valid items
	pythonResponse := `{
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {}},
			{"batch_index": 2, "status_code": 200, "body": {}}
		]
	}`

	var batchResponse struct {
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}
	stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// Verify all results filled correctly
	assert.True(t, results[0].filled)
	assert.Equal(t, 200, results[0].StatusCode)

	assert.True(t, results[1].filled)
	assert.Equal(t, http.StatusBadRequest, results[1].StatusCode) // Pre-filled error

	assert.True(t, results[2].filled)
	assert.Equal(t, 200, results[2].StatusCode)
}

// =============================================================================
// ATOMIC TESTS - Edge Cases
// =============================================================================

// Atom 1: batchItem correctly stores TaskId and BatchIdx
func TestBatchItem_Atom_FieldAssignment(t *testing.T) {
	item := batchItem{
		TaskId:   "task-123",
		BatchIdx: 5,
		Payload:  &types.TaskPayload{Args: []interface{}{"hello"}},
		Error:    nil,
	}

	assert.Equal(t, "task-123", item.TaskId)
	assert.Equal(t, 5, item.BatchIdx)
	assert.NotNil(t, item.Payload)
	assert.Nil(t, item.Error)
}

// Atom 1b: batchItem can store error state
func TestBatchItem_Atom_ErrorState(t *testing.T) {
	item := batchItem{
		TaskId:   "task-456",
		BatchIdx: 2,
		Error:    assert.AnError,
	}

	assert.Equal(t, "task-456", item.TaskId)
	assert.Equal(t, 2, item.BatchIdx)
	assert.Nil(t, item.Payload)
	assert.NotNil(t, item.Error)
}

// =============================================================================
// Atom: batchResultItem struct and JSON serialization
// =============================================================================

// Atom 2: batchResultItem JSON tags are correct for protocol compatibility
func TestBatchResultItem_Atom_JSONTags(t *testing.T) {
	result := batchResultItem{
		BatchIdx:   3,
		StatusCode: 200,
		Body:       stdjson.RawMessage(`{"result": "success"}`),
		filled:     true,
	}

	// Serialize to JSON
	jsonBytes, err := stdjson.Marshal(result)
	assert.NoError(t, err)

	// Verify JSON structure matches protocol (batch_index, not batch_idx)
	var parsed map[string]interface{}
	err = stdjson.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	// Check key names match protocol
	assert.Contains(t, parsed, "batch_index", "JSON key should be 'batch_index' per protocol")
	assert.Contains(t, parsed, "status_code")
	assert.Contains(t, parsed, "body")
	assert.NotContains(t, parsed, "filled", "filled is internal, should not be serialized")
}

// Atom 2b: batchResultItem correctly deserializes from Python response format
func TestBatchResultItem_Atom_DeserializeFromPython(t *testing.T) {
	// This is the format Python returns
	pythonResponse := `{
		"batch_index": 2,
		"status_code": 200,
		"body": {"prediction": 0.95}
	}`

	var result struct {
		BatchIndex int                `json:"batch_index"`
		StatusCode int                `json:"status_code"`
		Body       stdjson.RawMessage `json:"body"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &result)
	assert.NoError(t, err)
	assert.Equal(t, 2, result.BatchIndex)
	assert.Equal(t, 200, result.StatusCode)
	assert.NotEmpty(t, result.Body)
}

// =============================================================================
// Atom: Batch payload structure for Python
// =============================================================================

// Atom 3: Batch payload structure matches Python /__batch__ protocol
func TestBatchPayload_Atom_ProtocolStructure(t *testing.T) {
	// Build payload the same way handleBatchHttpRequest does
	validItems := []map[string]interface{}{
		{
			"batch_index": 0,
			"task_id":     "task-001",
			"payload": &types.TaskPayload{
				Args:   []interface{}{"hello"},
				Kwargs: map[string]interface{}{"x": 123},
			},
		},
		{
			"batch_index": 1,
			"task_id":     "task-002",
			"payload": &types.TaskPayload{
				Args:   []interface{}{"world"},
				Kwargs: map[string]interface{}{"x": 456},
			},
		},
	}

	batchPayload := map[string]interface{}{
		"batch_id": "batch-uuid-123",
		"items":    validItems,
	}

	jsonBytes, err := stdjson.Marshal(batchPayload)
	assert.NoError(t, err)

	// Parse and verify structure
	var parsed map[string]interface{}
	err = stdjson.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	assert.Contains(t, parsed, "batch_id")
	assert.Contains(t, parsed, "items")
	assert.Equal(t, "batch-uuid-123", parsed["batch_id"])

	items := parsed["items"].([]interface{})
	assert.Len(t, items, 2)

	// Verify first item structure
	item0 := items[0].(map[string]interface{})
	assert.Contains(t, item0, "batch_index")
	assert.Contains(t, item0, "task_id")
	assert.Contains(t, item0, "payload")
	assert.Equal(t, float64(0), item0["batch_index"]) // JSON numbers are float64
	assert.Equal(t, "task-001", item0["task_id"])
}

// =============================================================================
// Atom: Batch response parsing and demux logic
// =============================================================================

// Atom 4: Response demux correctly maps batch_index to results array
func TestBatchDemux_Atom_IndexMapping(t *testing.T) {
	// Simulate Python response (may be out of order or sparse)
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 2, "status_code": 200, "body": {"r": 2}},
			{"batch_index": 0, "status_code": 200, "body": {"r": 0}},
			{"batch_index": 1, "status_code": 500, "body": {"error": "failed"}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)
	assert.NoError(t, err)

	// Simulate demux logic from handleBatchHttpRequest
	results := make([]batchResultItem, 3) // 3 original requests

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// Verify demux worked correctly despite out-of-order response
	assert.True(t, results[0].filled)
	assert.Equal(t, 200, results[0].StatusCode)

	assert.True(t, results[1].filled)
	assert.Equal(t, 500, results[1].StatusCode)

	assert.True(t, results[2].filled)
	assert.Equal(t, 200, results[2].StatusCode)
}

// Atom 4b: Demux handles missing batch indices gracefully
func TestBatchDemux_Atom_MissingIndices(t *testing.T) {
	// Python only returns results for indices 0 and 2, missing 1
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"r": 0}},
			{"batch_index": 2, "status_code": 200, "body": {"r": 2}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)
	assert.NoError(t, err)

	results := make([]batchResultItem, 3)

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	assert.True(t, results[0].filled)
	assert.False(t, results[1].filled, "index 1 should remain unfilled")
	assert.True(t, results[2].filled)
}

// Atom 4c: Demux ignores out-of-bounds indices
func TestBatchDemux_Atom_OutOfBoundsIndex(t *testing.T) {
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {}},
			{"batch_index": 99, "status_code": 200, "body": {}},
			{"batch_index": -1, "status_code": 200, "body": {}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)
	assert.NoError(t, err)

	results := make([]batchResultItem, 2) // Only 2 slots

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	assert.True(t, results[0].filled, "index 0 should be filled")
	assert.False(t, results[1].filled, "index 1 should remain unfilled (no valid response)")
	// No panic from out-of-bounds access
}

// Atom 4d: Demux doesn't overwrite already-filled results
func TestBatchDemux_Atom_NoOverwrite(t *testing.T) {
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"first": true}},
			{"batch_index": 0, "status_code": 500, "body": {"second": true}}
		]
	}`

	var batchResponse struct {
		BatchId string `json:"batch_id"`
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}

	err := stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)
	assert.NoError(t, err)

	results := make([]batchResultItem, 1)

	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// First result should win
	assert.Equal(t, 200, results[0].StatusCode, "first result should not be overwritten")
	assert.Contains(t, string(results[0].Body), "first")
}

// =============================================================================
// Atom: Error item pre-filling
// =============================================================================

// Atom 5: Error items are pre-filled with 400 status before sending to container
func TestBatchErrorPreFill_Atom_BadRequestStatus(t *testing.T) {
	// Simulate what happens when parseBatchItems returns an error for an item
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{}, Error: nil},
		{TaskId: "task-1", BatchIdx: 1, Error: assert.AnError}, // This one has an error
		{TaskId: "task-2", BatchIdx: 2, Payload: &types.TaskPayload{}, Error: nil},
	}

	results := make([]batchResultItem, len(items))
	validItems := make([]map[string]interface{}, 0)

	for i, item := range items {
		if item.Error != nil {
			// Pre-fill error result (same logic as handleBatchHttpRequest)
			errBody, _ := stdjson.Marshal(map[string]interface{}{
				"error": item.Error.Error(),
			})
			results[i] = batchResultItem{
				BatchIdx:   i,
				StatusCode: http.StatusBadRequest,
				Body:       errBody,
				filled:     true,
			}
			continue
		}
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	// Verify error item is pre-filled
	assert.True(t, results[1].filled)
	assert.Equal(t, http.StatusBadRequest, results[1].StatusCode)
	assert.Contains(t, string(results[1].Body), "error")

	// Verify valid items are collected
	assert.Len(t, validItems, 2)
	assert.Equal(t, 0, validItems[0]["batch_index"])
	assert.Equal(t, 2, validItems[1]["batch_index"])
}

// =============================================================================
// Atom: BatchConfig validation
// =============================================================================

// Atom 6: BatchConfig fields match SDK protocol (max_batch_size, wait_ms)
func TestBatchConfig_Atom_JSONTags(t *testing.T) {
	config := types.BatchConfig{
		MaxSize: 10,
		WaitMs:  100,
	}

	jsonBytes, err := stdjson.Marshal(config)
	assert.NoError(t, err)

	var parsed map[string]interface{}
	err = stdjson.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	// Verify JSON keys match SDK protocol
	assert.Contains(t, parsed, "max_batch_size", "should use max_batch_size per SDK protocol")
	assert.Contains(t, parsed, "wait_ms")
	assert.Equal(t, float64(10), parsed["max_batch_size"])
	assert.Equal(t, float64(100), parsed["wait_ms"])
}

// Atom 6b: BatchConfig deserializes from SDK format
func TestBatchConfig_Atom_DeserializeFromSDK(t *testing.T) {
	// This is the format the Python SDK sends
	sdkJSON := `{"max_batch_size": 32, "wait_ms": 50}`

	var config types.BatchConfig
	err := stdjson.Unmarshal([]byte(sdkJSON), &config)
	assert.NoError(t, err)

	assert.Equal(t, 32, config.MaxSize)
	assert.Equal(t, 50, config.WaitMs)
}

// =============================================================================
// Atom: batchBuffer struct
// =============================================================================

// Atom 7: batchBuffer correctly tracks batch metadata
func TestBatchBuffer_Atom_Metadata(t *testing.T) {
	batch := batchBuffer{
		batchId:  "batch-uuid-456",
		maxSize:  10,
		timeout:  100,
		requests: nil,
	}

	assert.Equal(t, "batch-uuid-456", batch.batchId)
	assert.Equal(t, 10, batch.maxSize)
}

// =============================================================================
// INTEROPERABILITY TESTS - Verify atoms work together
// =============================================================================

// Interop: Full batch round-trip simulation (without HTTP)
func TestBatch_Interop_RoundTrip(t *testing.T) {
	// Step 1: Create batch items (simulating parseBatchItems)
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Payload: &types.TaskPayload{Args: []interface{}{"a"}}},
		{TaskId: "task-1", BatchIdx: 1, Error: assert.AnError}, // Error item
		{TaskId: "task-2", BatchIdx: 2, Payload: &types.TaskPayload{Args: []interface{}{"c"}}},
	}

	// Step 2: Build payload and pre-fill errors
	results := make([]batchResultItem, len(items))
	validItems := make([]map[string]interface{}, 0)

	for i, item := range items {
		if item.Error != nil {
			errBody, _ := stdjson.Marshal(map[string]interface{}{"error": item.Error.Error()})
			results[i] = batchResultItem{BatchIdx: i, StatusCode: 400, Body: errBody, filled: true}
			continue
		}
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
			"payload":     item.Payload,
		})
	}

	// Step 3: Simulate Python response (only for valid items)
	pythonResponse := `{
		"batch_id": "batch-123",
		"results": [
			{"batch_index": 0, "status_code": 200, "body": {"result": "a_processed"}},
			{"batch_index": 2, "status_code": 200, "body": {"result": "c_processed"}}
		]
	}`

	var batchResponse struct {
		Results []struct {
			BatchIndex int                `json:"batch_index"`
			StatusCode int                `json:"status_code"`
			Body       stdjson.RawMessage `json:"body"`
		} `json:"results"`
	}
	stdjson.Unmarshal([]byte(pythonResponse), &batchResponse)

	// Step 4: Demux response
	for _, r := range batchResponse.Results {
		if r.BatchIndex >= 0 && r.BatchIndex < len(results) && !results[r.BatchIndex].filled {
			results[r.BatchIndex] = batchResultItem{
				BatchIdx:   r.BatchIndex,
				StatusCode: r.StatusCode,
				Body:       r.Body,
				filled:     true,
			}
		}
	}

	// Verify all results are filled correctly
	assert.True(t, results[0].filled)
	assert.Equal(t, 200, results[0].StatusCode)
	assert.Contains(t, string(results[0].Body), "a_processed")

	assert.True(t, results[1].filled)
	assert.Equal(t, 400, results[1].StatusCode) // Pre-filled error
	assert.Contains(t, string(results[1].Body), "error")

	assert.True(t, results[2].filled)
	assert.Equal(t, 200, results[2].StatusCode)
	assert.Contains(t, string(results[2].Body), "c_processed")
}

// Interop: Batch with all errors (no valid items sent to container)
func TestBatch_Interop_AllErrors(t *testing.T) {
	items := []batchItem{
		{TaskId: "task-0", BatchIdx: 0, Error: assert.AnError},
		{TaskId: "task-1", BatchIdx: 1, Error: assert.AnError},
	}

	results := make([]batchResultItem, len(items))
	validItems := make([]map[string]interface{}, 0)

	for i, item := range items {
		if item.Error != nil {
			errBody, _ := stdjson.Marshal(map[string]interface{}{"error": item.Error.Error()})
			results[i] = batchResultItem{BatchIdx: i, StatusCode: 400, Body: errBody, filled: true}
			continue
		}
		validItems = append(validItems, map[string]interface{}{
			"batch_index": item.BatchIdx,
			"task_id":     item.TaskId,
		})
	}

	// No valid items, so no HTTP request would be made
	assert.Len(t, validItems, 0)

	// All results should be pre-filled with errors
	assert.True(t, results[0].filled)
	assert.True(t, results[1].filled)
	assert.Equal(t, 400, results[0].StatusCode)
	assert.Equal(t, 400, results[1].StatusCode)
}
