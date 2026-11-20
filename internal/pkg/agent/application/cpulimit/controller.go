// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cpulimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/elastic/elastic-agent/pkg/core/logger"
	"github.com/elastic/elastic-agent/pkg/limits"
)

const (
	// retryMinDelay is short so that a failed check is retried soon and the cap is applied as early as the host allows.
	// The delay doubles after each failure in a row, up to retryMaxDelay.
	retryMinDelay = 5 * time.Second
	retryMaxDelay = time.Minute
	// recheckInterval is the time between checks while a cap is applied, so a quota changed outside the agent is
	// corrected.
	recheckInterval = time.Minute
	callTimeout     = 30 * time.Second
	// shutdownRemoveTimeout limits the removal of the agent quota when Run stops, so the agent stops quickly.
	shutdownRemoveTimeout = 5 * time.Second
)

// Controller keeps the CPU quota in the operating system in line with the latest requested CPU limit.
// It runs one backend call at a time. A Set that changes the value does not stop a running call: the call ends or
// times out, its result is dropped, and the next pass corrects the quota. A Set with the same value lets the running
// pass finish and runs one more check after it.
// While a cap is requested and the last check passed, it checks again every minute and corrects the quota if needed.
// Failures are retried after 5 seconds, and the delay doubles after each failure in a row, up to one minute.
// A snapshot is published only when it changes.
type Controller struct {
	log     *logger.Logger
	backend Backend
	wake    chan struct{}
	updates chan Snapshot
	ready   chan struct{}
	// ownQuota is true when this controller called Apply and no cap-off pass has confirmed since that no agent quota
	// remains. A failed Apply counts too, because it may have written the quota.
	// Only Run uses it.
	ownQuota bool
	// foundOwned is true when the last successful inspection of a published pass found a quota owned by the agent.
	// Only Run uses it.
	foundOwned bool

	mu     sync.Mutex
	req    limits.CPULimit
	hasReq bool
	// gen grows when Set changes the value. A pass publishes its result only if gen did not change while it ran.
	gen uint64
	// changing is true while a requested cap is not checked yet or while a pass corrects the quota.
	changing bool
	// removing is true while an agent quota is being removed because the cap is off.
	removing    bool
	failed      bool
	unsupported bool
	reason      string
	err         error
	quota       Quota
	// cpus is the last logical CPU count, or 0 if it was never read.
	cpus        int
	readyClosed bool
	// retryDelay is the delay before the last retry, or 0 after a success or a change of the requested value.
	retryDelay time.Duration
	// loggedErr is the text of the last failure logged at Warn level. It is empty after a success.
	loggedErr string
}

// NewController returns a Controller that uses backend for all operating system calls.
func NewController(log *logger.Logger, backend Backend) *Controller {
	return &Controller{
		log:     log,
		backend: backend,
		wake:    make(chan struct{}, 1),
		updates: make(chan Snapshot, 1),
		ready:   make(chan struct{}),
	}
}

// Set records l as the latest accepted CPU limit and starts a new check, also when l did not change.
// It returns without waiting for the check.
func (c *Controller) Set(l limits.CPULimit) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.hasReq || l != c.req {
		c.gen++
		if l.Enabled() {
			c.changing = true
		}
		// The old result belongs to the old value.
		c.failed, c.err, c.unsupported, c.reason = false, nil, false, ""
		c.retryDelay = 0
	}
	c.req = l
	c.hasReq = true
	if !l.Enabled() {
		c.closeReadyLocked()
	}
	c.publishLocked()

	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Snapshot returns the current state of the CPU cap.
func (c *Controller) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

// Updates returns a channel that holds the newest Snapshot. A newer Snapshot replaces an unread one.
func (c *Controller) Updates() <-chan Snapshot {
	return c.updates
}

// Ready returns a channel that is closed the first time the cap is off, verified or unsupported.
// It stays closed after that.
func (c *Controller) Ready() <-chan struct{} {
	return c.ready
}

// Run reconciles the quota until ctx is done. It does nothing before the first Set.
// Call Run only once.
// When ctx is done, Run removes the quota this controller may own, so that none is left after the agent stops.
// This is best effort and takes at most 5 seconds. A crash cannot clean up, so the next start finds the leftover quota.
func (c *Controller) Run(ctx context.Context) error {
	defer c.removeOwnQuota()
	defer c.endRemoval()

	// timer starts the next retry or recheck. A Set replaces it with a pass at once.
	var timer *time.Timer
	var timerC <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		case <-timerC:
		}
		if ctx.Err() != nil {
			return nil
		}
		if timer != nil {
			timer.Stop()
			timerC = nil
		}

		if wait := c.reconcile(ctx); wait > 0 {
			timer = time.NewTimer(wait)
			timerC = timer.C
		}
	}
}

type passResult struct {
	// cpus is the logical CPU count read in the pass, or 0.
	cpus int
	// quota comes from the last inspection of the pass. checked is false when no inspection ran.
	// A failed inspection leaves quota unknown.
	quota   Quota
	checked bool
	// err is an unexpected failure.
	err error
	// unsupported is set when enforcement is unavailable by design.
	unsupported *UnsupportedError
	// inspected is true when an inspection of the pass succeeded. owned then tells whether the last such inspection
	// found a quota owned by the agent.
	inspected bool
	owned     bool
}

func (r *passResult) setOwned(ins Inspection) {
	r.inspected = true
	r.owned = ins.Owned && (ins.Quota.Kind == QuotaSet || ins.Unenforced)
}

// setErr stores err as unsupported when it reports that, and as an unexpected failure otherwise.
func (r *passResult) setErr(err error) {
	if !errors.As(err, &r.unsupported) {
		r.err = err
	}
}

func isUnsupported(err error) bool {
	var unsupported *UnsupportedError
	return errors.As(err, &unsupported)
}

// reconcile runs one pass for the latest request: inspect, apply or remove if needed, then verify.
// It returns the time to wait before the next pass, or 0 when only a Set starts the next pass.
func (c *Controller) reconcile(ctx context.Context) time.Duration {
	c.mu.Lock()
	req, gen := c.req, c.gen
	// Wakes sent before this point are covered by this pass, so they must not start another pass.
	select {
	case <-c.wake:
	default:
	}
	c.mu.Unlock()

	var res passResult
	var target float64
	if req.Enabled() {
		n, err := c.logicalCPUs(ctx)
		if err != nil {
			res.setErr(err)
			return c.finish(ctx, gen, res)
		}
		res.cpus = n
		target = req.EffectiveCPUs(n)
		if !c.current(ctx, gen) {
			return 0
		}
	}

	ins, err := c.inspect(ctx)
	res.checked = true
	if err != nil {
		switch {
		case !req.Enabled() && c.ownQuota:
			// The quota this controller applied may still be set.
			res.err = err
		case !req.Enabled() && !isUnsupported(err):
			// No removal is owed, so a check for a cap that is off does not fail. The next Set checks again.
			c.log.Debugw("Failed to inspect the CPU quota while the CPU limit is off", "error.message", err.Error())
		default:
			res.setErr(err)
		}
		return c.finish(ctx, gen, res)
	}
	res.quota = ins.Quota
	res.setOwned(ins)
	found := checkResult(req, ins, target, res.cpus)
	if found == nil {
		return c.finish(ctx, gen, res)
	}
	if !c.startChange(ctx, gen) {
		return 0
	}

	var timedOut bool
	var writeErr error
	if req.Enabled() {
		c.ownQuota = true
		timedOut, writeErr = c.write(ctx, func(ctx context.Context) error { return c.backend.Apply(ctx, target) })
	} else {
		timedOut, writeErr = c.write(ctx, c.backend.Remove)
	}
	if !c.current(ctx, gen) {
		return 0
	}

	ins, err = c.inspect(ctx)
	res.quota = ins.Quota
	if err == nil {
		res.setOwned(ins)
	}
	switch {
	case err != nil && (writeErr == nil || req.Enabled() && isUnsupported(writeErr)):
		// The verification may report that enforcement is unsupported.
		// With the cap off, the removal then counts as done because Remove returned no error.
		res.setErr(err)
	case err != nil:
		res.err = errors.Join(writeErr, err)
	case req.Enabled() && writeErr != nil && !timedOut:
		res.setErr(writeErr)
	default:
		// Only the inspection decides after a timeout or a removal: the quota may be right despite the error.
		checkErr := checkResult(req, ins, target, res.cpus)
		if checkErr != nil {
			res.err = errors.Join(writeErr, checkErr)
			break
		}
		if writeErr != nil {
			c.log.Warnw("The CPU quota is as expected although the change failed", "error.message", writeErr.Error())
		}
		c.log.Infow("Corrected a CPU quota different from the requested CPU limit", "found", found.Error())
	}
	return c.finish(ctx, gen, res)
}

// write runs f with a deadline. timedOut is true when f failed because a deadline passed.
func (c *Controller) write(ctx context.Context, f func(context.Context) error) (timedOut bool, err error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if err := f(callCtx); err != nil {
		// f can return the deadline error before callCtx reports it.
		timedOut = ctx.Err() == nil &&
			(errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded))
		return timedOut, fmt.Errorf("failed to change the CPU quota: %w", err)
	}
	return false, nil
}

func (c *Controller) logicalCPUs(ctx context.Context) (int, error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	n, err := c.backend.LogicalCPUs(callCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to read the logical CPU count: %w", err)
	}
	if n < 1 {
		return 0, fmt.Errorf("invalid logical CPU count %d", n)
	}
	return n, nil
}

func (c *Controller) inspect(ctx context.Context) (Inspection, error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	ins, err := c.backend.Inspect(callCtx)
	if err != nil {
		return Inspection{}, fmt.Errorf("failed to inspect the CPU quota: %w", err)
	}
	if ins.Unenforced {
		ins.Quota = Quota{}
	}
	// The snapshot is sent to Fleet as JSON, and encoding/json fails on NaN and Inf.
	if ins.Quota.Kind == QuotaSet && (math.IsNaN(ins.Quota.CPUs) || math.IsInf(ins.Quota.CPUs, 0)) {
		return Inspection{}, fmt.Errorf("invalid CPU quota %v", ins.Quota.CPUs)
	}
	return ins, nil
}

// checkResult returns nil when ins shows the intended result of req.
func checkResult(req limits.CPULimit, ins Inspection, target float64, logicalCPUs int) error {
	if ins.Unenforced {
		return errors.New("the agent CPU quota is not enforced")
	}
	if !req.Enabled() {
		if ins.Quota.Kind == QuotaSet && ins.Owned {
			return fmt.Errorf("the agent CPU quota of %.2f CPUs is still set", ins.Quota.CPUs)
		}
		return nil
	}
	if ins.Quota.Kind != QuotaSet {
		return fmt.Errorf("no CPU quota is set, expected %.2f CPUs", target)
	}
	// The tolerance covers platform rounding: 1% of one CPU for systemd, 1/10000 of the host for Windows.
	// The tolerance is inclusive, with a small margin for float rounding.
	// The check is written so that anything not within the tolerance, including an invalid number, is a mismatch.
	if !(math.Abs(ins.Quota.CPUs-target) <= max(0.01, float64(logicalCPUs)/10000)+1e-9) {
		return fmt.Errorf("the CPU quota is %.2f CPUs, expected %.2f CPUs", ins.Quota.CPUs, target)
	}
	return nil
}

func (c *Controller) current(ctx context.Context, gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ctx.Err() == nil && gen == c.gen
}

// startChange marks the change of the quota as running and publishes it. It returns false when the pass is stale.
// A stale pass leaves the flags to the next pass, which sets them again or clears them in finish.
func (c *Controller) startChange(ctx context.Context, gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil || gen != c.gen {
		return false
	}
	if c.req.Enabled() {
		c.changing = true
	} else {
		c.removing = true
	}
	c.publishLocked()
	return true
}

// endRemoval clears the removal flag left by a cancelled pass and publishes the result.
func (c *Controller) endRemoval() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.removing {
		c.removing = false
		c.publishLocked()
	}
}

// removeOwnQuota removes the quota this controller may own. It runs when Run stops, so ctx of Run is already done.
func (c *Controller) removeOwnQuota() {
	if !c.ownQuota && !c.foundOwned {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownRemoveTimeout)
	defer cancel()
	if err := c.backend.Remove(ctx); err != nil {
		c.log.Warnw("Failed to remove the CPU quota when stopping", "error.message", err.Error())
	}
}

// finish stores res if the pass is still current and publishes the snapshot if it changed.
// It returns the time to wait before the next pass, or 0 when only a Set starts the next pass.
func (c *Controller) finish(ctx context.Context, gen uint64, res passResult) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil || gen != c.gen {
		return 0
	}
	before := c.snapshotLocked()

	if res.cpus > 0 {
		c.cpus = res.cpus
	}
	if res.checked {
		c.quota = res.quota
	}
	if res.inspected {
		c.foundOwned = res.owned
	}
	c.changing = false
	c.removing = false

	switch {
	case res.unsupported != nil:
		if c.req.Enabled() && (!c.unsupported || c.reason != res.unsupported.Reason) {
			c.log.Infow("CPU limit enforcement is not supported on this host", "reason", res.unsupported.Reason)
		}
		c.failed, c.unsupported, c.err, c.reason = false, true, nil, res.unsupported.Reason
	case res.err != nil:
		c.failed, c.err = true, res.err
		if msg := res.err.Error(); msg != c.loggedErr {
			c.loggedErr = msg
			c.log.Warnw("Failed to enforce the CPU limit, will retry", "error.message", msg)
		} else {
			c.log.Debugw("Failed to enforce the CPU limit again, will retry", "error.message", msg)
		}
	default:
		if c.failed {
			c.log.Info("CPU limit enforcement recovered")
		}
		c.failed, c.unsupported, c.err, c.reason = false, false, nil, ""
	}
	if !c.failed {
		c.closeReadyLocked()
		c.retryDelay, c.loggedErr = 0, ""
		// A cap-off pass that inspected and did not fail has verified that no agent quota remains.
		if !c.req.Enabled() && res.inspected {
			c.ownQuota, c.foundOwned = false, false
		}
	}
	// Every other change of the state publishes at once, so before is the last published snapshot.
	if !reflect.DeepEqual(before, c.snapshotLocked()) {
		c.publishLocked()
	}

	switch {
	case c.failed:
		c.retryDelay = min(max(2*c.retryDelay, retryMinDelay), retryMaxDelay)
		return c.retryDelay
	case c.req.Enabled() && !c.unsupported:
		return recheckInterval
	default:
		return 0
	}
}

func (c *Controller) closeReadyLocked() {
	if !c.readyClosed {
		c.readyClosed = true
		close(c.ready)
	}
}

func (c *Controller) publishLocked() {
	s := c.snapshotLocked()
	select {
	case <-c.updates:
	default:
	}
	c.updates <- s
}

func (c *Controller) snapshotLocked() Snapshot {
	changing := c.changing
	if c.hasReq && !c.req.Enabled() {
		changing = c.removing
	}
	s := Snapshot{
		State: DeriveState(StateInput{
			CapRequested: c.req.Enabled(),
			Failed:       c.failed,
			Changing:     changing,
			Unsupported:  c.unsupported,
		}),
		Configured:  c.req,
		ActiveQuota: c.quota,
		Err:         c.err,
	}
	if c.cpus > 0 {
		s.EffectiveCPUs = c.req.EffectiveCPUs(c.cpus)
	}
	if s.State == StateUnsupported {
		s.Reason = c.reason
	}
	return s
}
