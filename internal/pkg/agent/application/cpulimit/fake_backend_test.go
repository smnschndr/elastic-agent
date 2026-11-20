// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cpulimit

import (
	"context"
	"fmt"
	"sync"
)

// fakeBackend models the quota of one group. A hook, when set, runs instead of the model.
// Hooks must be set before the controller runs. They run without the lock, so they may call setQuota and model.
type fakeBackend struct {
	mu    sync.Mutex
	cpus  int
	quota Quota
	owned bool
	// unenforced makes Inspect report the quota as not enforced. Any write clears it.
	unenforced bool
	calls      []string

	onLogicalCPUs func(ctx context.Context) (int, error)
	onInspect     func(ctx context.Context) (Inspection, error)
	onApply       func(ctx context.Context, cpus float64) error
	onRemove      func(ctx context.Context) error
}

func newFakeBackend(cpus int) *fakeBackend {
	return &fakeBackend{cpus: cpus, quota: Quota{Kind: QuotaNone}}
}

func (f *fakeBackend) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

// takeCalls returns the calls recorded since the last takeCalls.
func (f *fakeBackend) takeCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := f.calls
	f.calls = nil
	return calls
}

func (f *fakeBackend) setQuota(q Quota, owned bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quota, f.owned, f.unenforced = q, owned, false
}

// setUnenforced makes the current quota owned but not enforced.
func (f *fakeBackend) setUnenforced() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.owned, f.unenforced = true, true
}

func (f *fakeBackend) LogicalCPUs(ctx context.Context) (int, error) {
	f.record("cpus")
	if f.onLogicalCPUs != nil {
		return f.onLogicalCPUs(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cpus, nil
}

func (f *fakeBackend) Inspect(ctx context.Context) (Inspection, error) {
	f.record("inspect")
	if f.onInspect != nil {
		return f.onInspect(ctx)
	}
	return f.model(), nil
}

func (f *fakeBackend) model() Inspection {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Inspection{Quota: f.quota, Owned: f.owned, Unenforced: f.unenforced}
}

func (f *fakeBackend) Apply(ctx context.Context, cpus float64) error {
	f.record(fmt.Sprintf("apply %.2f", cpus))
	if f.onApply != nil {
		return f.onApply(ctx, cpus)
	}
	f.setQuota(quotaSet(cpus), true)
	return nil
}

func (f *fakeBackend) Remove(ctx context.Context) error {
	f.record("remove")
	if f.onRemove != nil {
		return f.onRemove(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.owned {
		f.quota, f.owned, f.unenforced = Quota{Kind: QuotaNone}, false, false
	}
	return nil
}

func quotaSet(cpus float64) Quota {
	return Quota{Kind: QuotaSet, CPUs: cpus}
}
