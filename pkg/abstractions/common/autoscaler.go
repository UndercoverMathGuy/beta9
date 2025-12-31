package abstractions

import (
	"context"
	"sync"
	"time"
)

type AutoscalerResult struct {
	DesiredContainers int
	ResultValid       bool
	CRIUScaling       bool
}

type Autoscaler[I IAutoscaledInstance, S AutoscalerSample] struct {
	instance         I
	mostRecentSample S
	sampleLock       sync.RWMutex // HIGH FIX #13: Protect mostRecentSample access
	sampleFunc       func(I) (S, error)
	scaleFunc        func(I, S) *AutoscalerResult
}

type IAutoscaler interface {
	Start(ctx context.Context)
}

const (
	windowSize int           = 60                                     // Number of samples in the sampling window
	sampleRate time.Duration = time.Duration(1000) * time.Millisecond // Time between samples
)

type AutoscalerSample interface{}

func NewAutoscaler[I IAutoscaledInstance, S AutoscalerSample](instance I, sampleFunc func(I) (S, error), scaleFunc func(I, S) *AutoscalerResult) *Autoscaler[I, S] {
	return &Autoscaler[I, S]{
		instance:   instance,
		sampleFunc: sampleFunc,
		scaleFunc:  scaleFunc,
	}
}

// Start the autoscaler
func (as *Autoscaler[I, S]) Start(ctx context.Context) {
	ticker := time.NewTicker(sampleRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample, err := as.sampleFunc(as.instance)
			if err != nil {
				continue
			}

			// HIGH FIX #13: Protect mostRecentSample write with lock
			as.sampleLock.Lock()
			as.mostRecentSample = sample
			as.sampleLock.Unlock()

			scaleResult := as.scaleFunc(as.instance, sample)
			if scaleResult != nil && scaleResult.ResultValid {
				as.instance.ConsumeScaleResult(scaleResult) // Send autoscaling result back to instance
			}
		}
	}
}

// GetMostRecentSample returns the most recent sample in a thread-safe manner
func (as *Autoscaler[I, S]) GetMostRecentSample() S {
	as.sampleLock.RLock()
	defer as.sampleLock.RUnlock()
	return as.mostRecentSample
}
