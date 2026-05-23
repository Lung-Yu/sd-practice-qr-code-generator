package main

import (
	"sync"
	"time"
)

// cbState represents the three states of the circuit breaker.
//
//	CLOSED   → normal operation, requests pass through
//	OPEN     → fast-fail, no connection attempts; transitions to HALF_OPEN after openTimeout
//	HALF_OPEN → one trial request allowed; success→CLOSED, failure→OPEN
type cbState int

const (
	cbClosed   cbState = iota
	cbOpen
	cbHalfOpen
)

func (s cbState) String() string {
	switch s {
	case cbClosed:
		return "closed"
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// CircuitBreaker is a per-node breaker that wraps outbound proxy calls.
//
// Failure definition: TCP connection error OR HTTP 5xx response from the node.
// 404 (cache miss) is intentionally NOT a failure — it is normal cache behaviour.
//
// When OPEN the router returns {"error":"circuit_open"} immediately, giving the
// client a clear signal to fall back to the backing store with rate-limiting
// rather than hammering a dead node (thundering herd protection).
type CircuitBreaker struct {
	mu          sync.Mutex
	state       cbState
	failures    int // consecutive failures while CLOSED
	lastFailure time.Time

	failThreshold int
	openTimeout   time.Duration
}

func NewCB(failThreshold int, openTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failThreshold: failThreshold,
		openTimeout:   openTimeout,
	}
}

// Allow reports whether the caller should attempt the request.
// It automatically transitions OPEN→HALF_OPEN once openTimeout has elapsed.
// Thread-safe; the entire check-and-transition is held under a single lock.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Since(cb.lastFailure) >= cb.openTimeout {
			// Transition to HALF_OPEN and allow exactly this one trial.
			cb.state = cbHalfOpen
			return true
		}
		return false
	case cbHalfOpen:
		// A trial is already in flight; block all others until it resolves.
		return false
	}
	return false
}

// RecordSuccess resets the breaker to CLOSED.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = cbClosed
	cb.failures = 0
}

// RecordFailure increments the failure count and opens the circuit once
// failThreshold is reached, or immediately if already HALF_OPEN.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.failThreshold || cb.state == cbHalfOpen {
		cb.state = cbOpen
	}
}

// Reset moves the breaker to CLOSED unconditionally.
// Called by the health checker when a node is confirmed alive again.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = cbClosed
	cb.failures = 0
}

func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state.String()
}

func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}
