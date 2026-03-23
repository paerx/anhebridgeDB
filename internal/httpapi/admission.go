package httpapi

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var (
	errAdmissionQueueFull = errors.New("ingress queue full")
	errAdmissionTimeout   = errors.New("ingress queue timeout")
)

type admissionController struct {
	tokens  chan struct{}
	queue   chan struct{}
	timeout time.Duration

	inflight      atomic.Int64
	queued        atomic.Int64
	rejectFull    atomic.Uint64
	rejectTimeout atomic.Uint64
}

func newAdmissionController(maxInFlight, maxQueue int, timeout time.Duration) *admissionController {
	if maxInFlight <= 0 {
		maxInFlight = 256
	}
	if maxQueue <= 0 {
		maxQueue = 1024
	}
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	return &admissionController{
		tokens:  make(chan struct{}, maxInFlight),
		queue:   make(chan struct{}, maxQueue),
		timeout: timeout,
	}
}

func (a *admissionController) acquire(ctx context.Context) (func(), error) {
	select {
	case a.queue <- struct{}{}:
		a.queued.Add(1)
	default:
		a.rejectFull.Add(1)
		return nil, errAdmissionQueueFull
	}
	timer := time.NewTimer(a.timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		a.dequeue()
		return nil, ctx.Err()
	case <-timer.C:
		a.dequeue()
		a.rejectTimeout.Add(1)
		return nil, errAdmissionTimeout
	case a.tokens <- struct{}{}:
		a.dequeue()
		a.inflight.Add(1)
		return func() {
			select {
			case <-a.tokens:
			default:
			}
			a.inflight.Add(-1)
		}, nil
	}
}

func (a *admissionController) dequeue() {
	select {
	case <-a.queue:
	default:
	}
	a.queued.Add(-1)
}

func (a *admissionController) metrics() map[string]any {
	return map[string]any{
		"ingress_inflight":         a.inflight.Load(),
		"ingress_queued":           a.queued.Load(),
		"ingress_reject_queuefull": a.rejectFull.Load(),
		"ingress_reject_timeout":   a.rejectTimeout.Load(),
		"ingress_max_inflight":     cap(a.tokens),
		"ingress_max_queue":        cap(a.queue),
		"ingress_queue_timeout_ms": a.timeout.Milliseconds(),
	}
}
