// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cpulimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-agent/pkg/core/logger/loggertest"
	"github.com/elastic/elastic-agent/pkg/limits"
)

const hostCPUs = 8

var (
	errBackend   = errors.New("backend failure")
	errNoSystemd = fmt.Errorf("probe: %w", &UnsupportedError{Reason: "no systemd"})
	quotaNone    = Quota{Kind: QuotaNone}
	capOff       = limits.CPULimit{}
)

func TestControllerAppliesCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect"}, f.takeCalls())
		require.True(t, isClosed(c.Ready()))
		requireLastUpdate(t, c)
	})
}

func TestControllerBeforeFirstSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		c, stop := start(t, f)

		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDisabled, Configured: capOff}, false, c.Snapshot())
		require.False(t, isClosed(c.Ready()))
		stop()
		require.Empty(t, f.takeCalls())
	})
}

func TestControllerSkipsWriteWhenQuotaIsInPlace(t *testing.T) {
	cases := []struct {
		name      string
		cpus      int
		percent   int
		quota     float64
		owned     bool
		wantCalls []string
		wantQuota float64
	}{
		{name: "exact", cpus: 8, percent: 50, quota: 4, owned: true, wantCalls: []string{"cpus", "inspect"}, wantQuota: 4},
		{name: "within tolerance, not owned", cpus: 8, percent: 50, quota: 4.009, wantCalls: []string{"cpus", "inspect"}, wantQuota: 4.009},
		{
			name: "outside tolerance", cpus: 8, percent: 50, quota: 4.02, owned: true,
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"}, wantQuota: 4,
		},
		{name: "within tolerance of a large host", cpus: 1000, percent: 1, quota: 10.09, owned: true, wantCalls: []string{"cpus", "inspect"}, wantQuota: 10.09},
		{
			name: "outside tolerance of a large host", cpus: 1000, percent: 1, quota: 10.11, owned: true,
			wantCalls: []string{"cpus", "inspect", "apply 10.00", "inspect"}, wantQuota: 10,
		},
		{name: "at the tolerance boundary", cpus: 10, percent: 11, quota: 1.11, owned: true, wantCalls: []string{"cpus", "inspect"}, wantQuota: 1.11},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(tc.cpus)
				f.setQuota(quotaSet(tc.quota), tc.owned)
				c, _ := start(t, f)

				c.Set(cpuLimit(t, tc.percent))
				synctest.Wait()
				require.Equal(t, tc.wantCalls, f.takeCalls())
				s := c.Snapshot()
				require.Equal(t, StateApplied, s.State)
				require.Equal(t, quotaSet(tc.wantQuota), s.ActiveQuota)
			})
		})
	}
}

func TestControllerLatestRequestWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		f.onApply = func(ctx context.Context, cpus float64) error {
			if cpus == 4 {
				if err := wait(ctx, release); err != nil {
					return err
				}
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		c.Set(cpuLimit(t, 25))
		c.Set(cpuLimit(t, 75))
		synctest.Wait()
		require.Equal(t, StatePending, c.Snapshot().State)

		close(release)
		synctest.Wait()
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "cpus", "inspect", "apply 6.00", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 75), EffectiveCPUs: 6, ActiveQuota: quotaSet(6),
		}, false, c.Snapshot())
		requireLastUpdate(t, c)
	})
}

func TestControllerStalePassDoesNotPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		releaseVerify := make(chan struct{})
		releaseCount := make(chan struct{})
		passes := 0
		f.onLogicalCPUs = func(ctx context.Context) (int, error) {
			passes++
			if passes == 2 {
				if err := wait(ctx, releaseCount); err != nil {
					return 0, err
				}
			}
			return hostCPUs, nil
		}
		inspects := 0
		f.onInspect = func(ctx context.Context) (Inspection, error) {
			inspects++
			if inspects == 2 {
				if err := wait(ctx, releaseVerify); err != nil {
					return Inspection{}, err
				}
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		c.Set(cpuLimit(t, 25))
		require.Equal(t, cpuLimit(t, 25), (<-c.Updates()).Configured)
		close(releaseVerify)
		synctest.Wait()
		// The pass for 50 verified its quota after the change to 25. The pass for 25 waits for the CPU count.
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect", "cpus"}, f.takeCalls())
		require.Empty(t, c.Updates(), "the stale pass must not publish")
		requireSnapshot(t, Snapshot{State: StatePending, Configured: cpuLimit(t, 25)}, false, c.Snapshot())

		close(releaseCount)
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 25), EffectiveCPUs: 2, ActiveQuota: quotaSet(2),
		}, false, c.Snapshot())
	})
}

func TestControllerTurnsOffWhileApplyRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		f.onApply = func(ctx context.Context, cpus float64) error {
			if err := wait(ctx, release); err != nil {
				return err
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		c.Set(capOff)
		close(release)
		synctest.Wait()
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect", "remove", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())
		require.Equal(t, Inspection{Quota: quotaNone}, f.model())
		requireLastUpdate(t, c)
	})
}

func TestControllerStalePassStopsAfterCPUCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		first := true
		f.onLogicalCPUs = func(ctx context.Context) (int, error) {
			if first {
				first = false
				if err := wait(ctx, release); err != nil {
					return 0, err
				}
			}
			return hostCPUs, nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		c.Set(cpuLimit(t, 25))
		close(release)
		synctest.Wait()
		require.Equal(t, []string{"cpus", "cpus", "inspect", "apply 2.00", "inspect"}, f.takeCalls())
		require.Equal(t, StateApplied, c.Snapshot().State)
	})
}

func TestControllerRetryUsesLatestRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.onApply = func(context.Context, float64) error { return errBackend }
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		require.Equal(t, StateDegraded, c.Snapshot().State)
		f.takeCalls()

		c.Set(capOff)
		synctest.Wait()
		require.Equal(t, []string{"inspect"}, f.takeCalls())
		time.Sleep(recheckInterval)
		synctest.Wait()
		require.Empty(t, f.takeCalls())
		requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())
	})
}

func TestControllerRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.onApply = func(context.Context, float64) error { return errBackend }
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		require.Equal(t, 1, countApplies(f.takeCalls()))

		for _, seconds := range []int{5, 10, 20, 40, 60, 60} {
			requireApplyAfter(t, f, time.Duration(seconds)*time.Second)
		}

		require.Equal(t, StateDegraded, c.Snapshot().State)
		require.ErrorIs(t, c.Snapshot().Err, errBackend)
		require.False(t, isClosed(c.Ready()))
		c.Set(capOff)
		require.True(t, isClosed(c.Ready()))
	})
}

func TestControllerRetriesUntilApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		failures := 1
		f.onApply = func(_ context.Context, cpus float64) error {
			if failures > 0 {
				failures--
				return errBackend
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StateDegraded, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaNone,
		}, true, c.Snapshot())
		require.ErrorIs(t, c.Snapshot().Err, errBackend)
		require.False(t, isClosed(c.Ready()))

		time.Sleep(retryMinDelay)
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
		require.True(t, isClosed(c.Ready()))
	})
}

func TestControllerInspectsAfterTimeout(t *testing.T) {
	cases := []struct {
		name      string
		worked    bool
		atOnce    bool
		wantState State
		wantQuota Quota
	}{
		{name: "write worked", worked: true, wantState: StateApplied, wantQuota: quotaSet(4)},
		{name: "write did not work", wantState: StateDegraded, wantQuota: quotaNone},
		{name: "deadline error at once, write worked", worked: true, atOnce: true, wantState: StateApplied, wantQuota: quotaSet(4)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				f.onApply = func(ctx context.Context, cpus float64) error {
					if tc.worked {
						f.setQuota(quotaSet(cpus), true)
					}
					if tc.atOnce {
						return context.DeadlineExceeded
					}
					<-ctx.Done()
					return ctx.Err()
				}
				c, _ := start(t, f)

				c.Set(cpuLimit(t, 50))
				if !tc.atOnce {
					synctest.Wait()
					require.Equal(t, StatePending, c.Snapshot().State)
					time.Sleep(callTimeout)
				}
				synctest.Wait()
				require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect"}, f.takeCalls())
				requireSnapshot(t, Snapshot{
					State: tc.wantState, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: tc.wantQuota,
				}, !tc.worked, c.Snapshot())
				require.Equal(t, tc.worked, isClosed(c.Ready()))
			})
		})
	}
}

func TestControllerReportsFailures(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(f *fakeBackend)
		wantCalls []string
		want      Snapshot
		errIs     error
	}{
		{
			name:      "CPU count read fails",
			setup:     func(f *fakeBackend) { f.onLogicalCPUs = func(context.Context) (int, error) { return 0, errBackend } },
			wantCalls: []string{"cpus"},
			errIs:     errBackend,
		},
		{
			name:      "CPU count is zero",
			setup:     func(f *fakeBackend) { f.onLogicalCPUs = func(context.Context) (int, error) { return 0, nil } },
			wantCalls: []string{"cpus"},
		},
		{
			name: "inspect fails",
			setup: func(f *fakeBackend) {
				f.onInspect = func(context.Context) (Inspection, error) { return Inspection{}, errBackend }
			},
			wantCalls: []string{"cpus", "inspect"},
			want:      Snapshot{EffectiveCPUs: 4},
			errIs:     errBackend,
		},
		{
			name: "apply sets a wrong quota",
			setup: func(f *fakeBackend) {
				f.onApply = func(context.Context, float64) error {
					f.setQuota(quotaSet(3), true)
					return nil
				}
			},
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{EffectiveCPUs: 4, ActiveQuota: quotaSet(3)},
		},
		{
			name: "inspect returns NaN",
			setup: func(f *fakeBackend) {
				f.onInspect = func(context.Context) (Inspection, error) {
					return Inspection{Quota: quotaSet(math.NaN()), Owned: true}, nil
				}
			},
			wantCalls: []string{"cpus", "inspect"},
			want:      Snapshot{EffectiveCPUs: 4},
		},
		{
			name: "inspect returns Inf",
			setup: func(f *fakeBackend) {
				f.onInspect = func(context.Context) (Inspection, error) {
					return Inspection{Quota: quotaSet(math.Inf(1)), Owned: true}, nil
				}
			},
			wantCalls: []string{"cpus", "inspect"},
			want:      Snapshot{EffectiveCPUs: 4},
		},
		{
			name: "apply leaves the quota unenforced",
			setup: func(f *fakeBackend) {
				// The controller must ignore Quota when Unenforced is set.
				f.setQuota(quotaSet(4), true)
				f.setUnenforced()
				f.onApply = func(context.Context, float64) error { return nil }
			},
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{EffectiveCPUs: 4},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				tc.setup(f)
				c, _ := start(t, f)

				c.Set(cpuLimit(t, 50))
				synctest.Wait()
				require.Equal(t, tc.wantCalls, f.takeCalls())
				want := tc.want
				want.State = StateDegraded
				want.Configured = cpuLimit(t, 50)
				requireSnapshot(t, want, true, c.Snapshot())
				if tc.errIs != nil {
					require.ErrorIs(t, c.Snapshot().Err, tc.errIs)
				}
				require.False(t, isClosed(c.Ready()))
			})
		})
	}
}

func TestControllerChangePendingKeepsMeasuredQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		f.onApply = func(ctx context.Context, cpus float64) error {
			if cpus == 2 {
				if err := wait(ctx, release); err != nil {
					return err
				}
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()

		c.Set(cpuLimit(t, 25))
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State:         StatePending,
			Configured:    cpuLimit(t, 25),
			EffectiveCPUs: 2,
			ActiveQuota:   quotaSet(4),
		}, false, c.Snapshot())

		close(release)
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 25), EffectiveCPUs: 2, ActiveQuota: quotaSet(2),
		}, false, c.Snapshot())
	})
}

func TestControllerRecheckFixesDrift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		applies := 0
		f.onApply = func(ctx context.Context, cpus float64) error {
			applies++
			if applies == 2 {
				if err := wait(ctx, release); err != nil {
					return err
				}
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		before := c.Snapshot()
		f.takeCalls()
		f.setQuota(quotaSet(6), false)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		want := before
		want.State = StatePending
		require.Equal(t, want, c.Snapshot(), "a correction is pending and keeps the last measured quota")

		close(release)
		synctest.Wait()
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
	})
}

func TestControllerTurnsCapOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		f.onRemove = func(ctx context.Context) error {
			if err := wait(ctx, release); err != nil {
				return err
			}
			f.setQuota(quotaNone, false)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()

		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{
			State: StatePending, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
		requireLastUpdate(t, c)

		close(release)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "remove", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())
		requireLastUpdate(t, c)

		time.Sleep(recheckInterval)
		synctest.Wait()
		require.Empty(t, f.takeCalls(), "no recheck with the cap off")
	})
}

func TestControllerCapOffAtStart(t *testing.T) {
	cases := []struct {
		name      string
		quota     Quota
		owned     bool
		wantCalls []string
		wantQuota Quota
	}{
		{name: "nothing to remove", quota: quotaNone, wantCalls: []string{"inspect"}, wantQuota: quotaNone},
		{name: "quota not owned", quota: quotaSet(2), wantCalls: []string{"inspect"}, wantQuota: quotaSet(2)},
		{name: "owned leftover", quota: quotaSet(2), owned: true, wantCalls: []string{"inspect", "remove", "inspect"}, wantQuota: quotaNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				f.setQuota(tc.quota, tc.owned)
				release := make(chan struct{})
				first := true
				f.onInspect = func(ctx context.Context) (Inspection, error) {
					if first {
						first = false
						if err := wait(ctx, release); err != nil {
							return Inspection{}, err
						}
					}
					return f.model(), nil
				}
				c, _ := start(t, f)

				c.Set(capOff)
				require.True(t, isClosed(c.Ready()))
				synctest.Wait()
				requireSnapshot(t, Snapshot{State: StateDisabled}, false, c.Snapshot())

				close(release)
				synctest.Wait()
				require.Equal(t, tc.wantCalls, f.takeCalls())
				requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: tc.wantQuota}, false, c.Snapshot())
			})
		})
	}
}

func TestControllerRemovalFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "unexpected failure", err: errBackend},
		{name: "unsupported", err: errNoSystemd},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				f.setQuota(quotaSet(2), true)
				failures := 1
				f.onRemove = func(context.Context) error {
					if failures > 0 {
						failures--
						return tc.err
					}
					f.setQuota(quotaNone, false)
					return nil
				}
				c, _ := start(t, f)

				c.Set(capOff)
				synctest.Wait()
				requireSnapshot(t, Snapshot{State: StateDegraded, ActiveQuota: quotaSet(2)}, true, c.Snapshot())
				require.ErrorIs(t, c.Snapshot().Err, tc.err)
				require.True(t, isClosed(c.Ready()))
				f.takeCalls()

				time.Sleep(retryMinDelay)
				synctest.Wait()
				require.Equal(t, []string{"inspect", "remove", "inspect"}, f.takeCalls())
				requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())
			})
		})
	}
}

func TestControllerUnsupported(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		setup     func(f *fakeBackend)
		wantCalls []string
		want      Snapshot
	}{
		{
			name:      "CPU count unsupported with cap on",
			limit:     50,
			setup:     func(f *fakeBackend) { f.onLogicalCPUs = func(context.Context) (int, error) { return 0, errNoSystemd } },
			wantCalls: []string{"cpus"},
			want:      Snapshot{State: StateUnsupported, Reason: "no systemd"},
		},
		{
			name:  "inspect unsupported with cap on",
			limit: 50,
			setup: func(f *fakeBackend) {
				f.onInspect = func(context.Context) (Inspection, error) { return Inspection{}, errNoSystemd }
			},
			wantCalls: []string{"cpus", "inspect"},
			want:      Snapshot{State: StateUnsupported, EffectiveCPUs: 4, Reason: "no systemd"},
		},
		{
			name:      "apply unsupported with cap on",
			limit:     50,
			setup:     func(f *fakeBackend) { f.onApply = func(context.Context, float64) error { return errNoSystemd } },
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{State: StateUnsupported, EffectiveCPUs: 4, ActiveQuota: quotaNone, Reason: "no systemd"},
		},
		{
			name:  "verification unsupported with cap on",
			limit: 50,
			setup: func(f *fakeBackend) {
				inspects := 0
				f.onInspect = func(context.Context) (Inspection, error) {
					inspects++
					if inspects == 2 {
						return Inspection{}, errNoSystemd
					}
					return f.model(), nil
				}
			},
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{State: StateUnsupported, EffectiveCPUs: 4, Reason: "no systemd"},
		},
		{
			name:  "apply and verification unsupported with cap on",
			limit: 50,
			setup: func(f *fakeBackend) {
				inspects := 0
				f.onInspect = func(context.Context) (Inspection, error) {
					inspects++
					if inspects == 2 {
						return Inspection{}, errNoSystemd
					}
					return f.model(), nil
				}
				f.onApply = func(context.Context, float64) error { return errNoSystemd }
			},
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{State: StateUnsupported, EffectiveCPUs: 4, Reason: "no systemd"},
		},
		{
			name: "inspect unsupported with cap off",
			setup: func(f *fakeBackend) {
				f.onInspect = func(context.Context) (Inspection, error) { return Inspection{}, errNoSystemd }
			},
			wantCalls: []string{"inspect"},
			want:      Snapshot{State: StateDisabled},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				tc.setup(f)
				c, _ := start(t, f)

				limit := capOff
				if tc.limit > 0 {
					limit = cpuLimit(t, tc.limit)
				}
				c.Set(limit)
				synctest.Wait()
				require.Equal(t, tc.wantCalls, f.takeCalls())
				want := tc.want
				want.Configured = limit
				requireSnapshot(t, want, false, c.Snapshot())
				require.True(t, isClosed(c.Ready()))

				time.Sleep(recheckInterval)
				synctest.Wait()
				require.Empty(t, f.takeCalls(), "unsupported is not retried or rechecked")
			})
		})
	}
}

func TestControllerShutdownDuringHangingCall(t *testing.T) {
	hang := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	cases := []struct {
		name      string
		limit     int
		setup     func(f *fakeBackend)
		wantCalls []string
		want      Snapshot
	}{
		{
			name:      "CPU count",
			limit:     50,
			setup:     func(f *fakeBackend) { f.onLogicalCPUs = func(ctx context.Context) (int, error) { return 0, hang(ctx) } },
			wantCalls: []string{"cpus"},
			want:      Snapshot{State: StatePending},
		},
		{
			name:  "inspect",
			limit: 50,
			setup: func(f *fakeBackend) {
				f.onInspect = func(ctx context.Context) (Inspection, error) { return Inspection{}, hang(ctx) }
			},
			wantCalls: []string{"cpus", "inspect"},
			want:      Snapshot{State: StatePending},
		},
		{
			name:  "apply",
			limit: 50,
			setup: func(f *fakeBackend) { f.onApply = func(ctx context.Context, _ float64) error { return hang(ctx) } },
			// The apply may have written the quota, so Run removes it when it stops.
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "remove"},
			want:      Snapshot{State: StatePending},
		},
		{
			name: "remove",
			setup: func(f *fakeBackend) {
				f.setQuota(quotaSet(2), true)
				f.onRemove = hang
			},
			wantCalls: []string{"inspect", "remove"},
			want:      Snapshot{State: StateDisabled},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				tc.setup(f)
				c, stop := start(t, f)

				limit := capOff
				if tc.limit > 0 {
					limit = cpuLimit(t, tc.limit)
				}
				c.Set(limit)
				synctest.Wait()
				require.Equal(t, StatePending, c.Snapshot().State)
				stop()

				want := tc.want
				want.Configured = limit
				requireSnapshot(t, want, false, c.Snapshot())
				requireLastUpdate(t, c)

				// Set still returns after Run ended, and the newest Snapshot replaces an unread one.
				c.Set(cpuLimit(t, 25))
				c.Set(cpuLimit(t, 75))
				synctest.Wait()
				s := <-c.Updates()
				require.Equal(t, StatePending, s.State)
				require.Equal(t, cpuLimit(t, 75), s.Configured)
				require.Empty(t, c.Updates())
				require.Equal(t, tc.wantCalls, f.takeCalls())
			})
		})
	}
}

func TestControllerSameValueSetDoesNotStarve(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.onInspect = func(ctx context.Context) (Inspection, error) {
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return Inspection{}, ctx.Err()
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		for range 20 {
			c.Set(cpuLimit(t, 50))
			time.Sleep(100 * time.Millisecond)
		}
		synctest.Wait()
		s := c.Snapshot()
		require.Equal(t, StateApplied, s.State)
		require.Equal(t, quotaSet(4), s.ActiveQuota)
		require.True(t, isClosed(c.Ready()))
		requireLastUpdate(t, c)
	})
}

func TestControllerChangeClearsOldFailure(t *testing.T) {
	cases := []struct {
		name        string
		next        int
		wantPending Snapshot
		wantDone    Snapshot
	}{
		{
			name:        "another cap",
			next:        25,
			wantPending: Snapshot{State: StatePending, EffectiveCPUs: 2, ActiveQuota: quotaNone},
			wantDone:    Snapshot{State: StateApplied, EffectiveCPUs: 2, ActiveQuota: quotaSet(2)},
		},
		{
			name:        "cap off",
			wantPending: Snapshot{State: StateDisabled, ActiveQuota: quotaNone},
			wantDone:    Snapshot{State: StateDisabled, ActiveQuota: quotaNone},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				f.onApply = func(_ context.Context, cpus float64) error {
					if cpus == 4 {
						return errBackend
					}
					f.setQuota(quotaSet(cpus), true)
					return nil
				}
				release := make(chan struct{})
				inspects := 0
				f.onInspect = func(ctx context.Context) (Inspection, error) {
					inspects++
					if inspects == 3 {
						if err := wait(ctx, release); err != nil {
							return Inspection{}, err
						}
					}
					return f.model(), nil
				}
				c, _ := start(t, f)

				c.Set(cpuLimit(t, 50))
				synctest.Wait()
				require.Equal(t, StateDegraded, c.Snapshot().State)
				next := capOff
				if tc.next > 0 {
					next = cpuLimit(t, tc.next)
				}
				c.Set(next)
				synctest.Wait()
				want := tc.wantPending
				want.Configured = next
				requireSnapshot(t, want, false, c.Snapshot())

				close(release)
				synctest.Wait()
				want = tc.wantDone
				want.Configured = next
				requireSnapshot(t, want, false, c.Snapshot())
			})
		})
	}
}

func TestControllerUnenforcedQuota(t *testing.T) {
	cases := []struct {
		name      string
		limit     int
		wantCalls []string
		want      Snapshot
	}{
		{
			name: "cap on", limit: 50,
			wantCalls: []string{"cpus", "inspect", "apply 4.00", "inspect"},
			want:      Snapshot{State: StateApplied, EffectiveCPUs: 4, ActiveQuota: quotaSet(4)},
		},
		{
			name:      "cap off",
			wantCalls: []string{"inspect", "remove", "inspect"},
			want:      Snapshot{State: StateDisabled, ActiveQuota: quotaNone},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				// The controller must ignore Quota when Unenforced is set.
				f.setQuota(quotaSet(4), true)
				f.setUnenforced()
				c, _ := start(t, f)

				limit := capOff
				if tc.limit > 0 {
					limit = cpuLimit(t, tc.limit)
				}
				c.Set(limit)
				synctest.Wait()
				require.Equal(t, tc.wantCalls, f.takeCalls())
				want := tc.want
				want.Configured = limit
				requireSnapshot(t, want, false, c.Snapshot())
			})
		})
	}
}

func TestControllerRemovalAfterApplyWhenInspectIsUnsupported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		var unsupported atomic.Bool
		f.onInspect = func(context.Context) (Inspection, error) {
			if unsupported.Load() {
				return Inspection{}, errNoSystemd
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		require.Equal(t, StateApplied, c.Snapshot().State)
		f.takeCalls()

		unsupported.Store(true)
		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDegraded}, true, c.Snapshot())
		require.ErrorIs(t, c.Snapshot().Err, errNoSystemd)
		require.True(t, isClosed(c.Ready()))

		time.Sleep(retryMinDelay)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "inspect"}, f.takeCalls(), "the failure is retried")
		require.Equal(t, StateDegraded, c.Snapshot().State)

		unsupported.Store(false)
		time.Sleep(2 * retryMinDelay)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "remove", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())

		// The removal is verified, so an unsupported host no longer has an agent quota to remove.
		unsupported.Store(true)
		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDisabled}, false, c.Snapshot())
		time.Sleep(recheckInterval)
		synctest.Wait()
		require.Equal(t, []string{"inspect"}, f.takeCalls())
	})
}

func TestControllerRemovalAfterTimedOutApplyWhenInspectIsUnsupported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.onApply = func(ctx context.Context, cpus float64) error {
			f.setQuota(quotaSet(cpus), true)
			<-ctx.Done()
			return ctx.Err()
		}
		var unsupported atomic.Bool
		f.onInspect = func(context.Context) (Inspection, error) {
			if unsupported.Load() {
				return Inspection{}, errNoSystemd
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		time.Sleep(callTimeout)
		synctest.Wait()
		require.Equal(t, StateApplied, c.Snapshot().State)
		f.takeCalls()

		unsupported.Store(true)
		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDegraded}, true, c.Snapshot())
		require.ErrorIs(t, c.Snapshot().Err, errNoSystemd)

		time.Sleep(retryMinDelay)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "inspect"}, f.takeCalls(), "the failure is retried")
		require.Equal(t, StateDegraded, c.Snapshot().State)

		unsupported.Store(false)
		time.Sleep(2 * retryMinDelay)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "remove", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{State: StateDisabled, ActiveQuota: quotaNone}, false, c.Snapshot())
	})
}

func TestControllerCapOffInspectFailureWithoutOwnQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.onInspect = func(context.Context) (Inspection, error) { return Inspection{}, errBackend }
		c, _ := start(t, f)

		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDisabled}, false, c.Snapshot())
		require.True(t, isClosed(c.Ready()))

		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, []string{"inspect"}, f.takeCalls(), "the check is not retried")

		c.Set(capOff)
		synctest.Wait()
		require.Equal(t, []string{"inspect"}, f.takeCalls())
		require.Equal(t, StateDisabled, c.Snapshot().State)
	})
}

func TestControllerCapOffInspectFailureWithOwnQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		var failing atomic.Bool
		f.onInspect = func(context.Context) (Inspection, error) {
			if failing.Load() {
				return Inspection{}, errBackend
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()

		failing.Store(true)
		c.Set(capOff)
		synctest.Wait()
		requireSnapshot(t, Snapshot{State: StateDegraded}, true, c.Snapshot())
		require.ErrorIs(t, c.Snapshot().Err, errBackend)

		time.Sleep(retryMinDelay)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "inspect"}, f.takeCalls(), "the failure is retried")
		require.Equal(t, StateDegraded, c.Snapshot().State)
	})
}

func TestControllerRemovalVerificationUnsupported(t *testing.T) {
	cases := []struct {
		name      string
		removeErr error
		want      Snapshot
		wantErr   bool
	}{
		{name: "remove worked", want: Snapshot{State: StateDisabled}},
		{name: "remove failed", removeErr: errBackend, want: Snapshot{State: StateDegraded}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				inspects := 0
				f.onInspect = func(context.Context) (Inspection, error) {
					inspects++
					// The verification after Remove and every later inspection report unsupported.
					if inspects >= 4 {
						return Inspection{}, errNoSystemd
					}
					return f.model(), nil
				}
				f.onRemove = func(context.Context) error {
					if tc.removeErr != nil {
						return tc.removeErr
					}
					f.setQuota(quotaNone, false)
					return nil
				}
				c, _ := start(t, f)

				c.Set(cpuLimit(t, 50))
				synctest.Wait()
				f.takeCalls()

				c.Set(capOff)
				synctest.Wait()
				require.Equal(t, []string{"inspect", "remove", "inspect"}, f.takeCalls())
				want := tc.want
				requireSnapshot(t, want, tc.wantErr, c.Snapshot())

				time.Sleep(retryMinDelay)
				synctest.Wait()
				if tc.wantErr {
					require.Equal(t, []string{"inspect"}, f.takeCalls())
					require.Equal(t, StateDegraded, c.Snapshot().State)
				} else {
					require.Empty(t, f.takeCalls())
				}
			})
		})
	}
}

func TestControllerSetDuringFirstInspectStopsRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		f.setQuota(quotaSet(2), true)
		release := make(chan struct{})
		first := true
		f.onInspect = func(ctx context.Context) (Inspection, error) {
			if first {
				first = false
				if err := wait(ctx, release); err != nil {
					return Inspection{}, err
				}
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(capOff)
		synctest.Wait()
		c.Set(cpuLimit(t, 50))
		close(release)
		synctest.Wait()
		require.Equal(t, []string{"inspect", "cpus", "inspect", "apply 4.00", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
	})
}

func TestControllerUpdatesWithConcurrentReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		c, _ := start(t, f)

		var mu sync.Mutex
		var last Snapshot
		done := make(chan struct{})
		go func() {
			for {
				select {
				case s := <-c.Updates():
					mu.Lock()
					last = s
					mu.Unlock()
				case <-done:
					return
				}
			}
		}()

		for i := range 200 {
			c.Set(cpuLimit(t, 1+i%100))
			if i%10 == 0 {
				c.Set(capOff)
			}
		}
		synctest.Wait()
		mu.Lock()
		require.Equal(t, c.Snapshot(), last)
		mu.Unlock()
		close(done)
	})
}

func TestControllerRunDoesNotStartPassAfterCancel(t *testing.T) {
	// When the wake and the cancelled ctx are both ready, select picks one at random, so the test repeats.
	for range 20 {
		f := newFakeBackend(hostCPUs)
		log, _ := loggertest.New(t.Name())
		c := NewController(log, f)
		c.Set(cpuLimit(t, 50))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, c.Run(ctx))
		require.Empty(t, f.takeCalls())
	}
}

func TestControllerExitRemovesOwnQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		var removeCtxErrs []error
		f.onRemove = func(ctx context.Context) error {
			removeCtxErrs = append(removeCtxErrs, ctx.Err())
			return nil
		}
		c, stop := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()
		stop()
		require.Equal(t, []string{"remove"}, f.takeCalls())
		require.Equal(t, []error{nil}, removeCtxErrs, "Remove gets a context that is not cancelled")
	})
}

func TestControllerExitRemoval(t *testing.T) {
	cases := []struct {
		name string
		// prepare sets up f before Run starts and returns the steps to run before Run stops.
		prepare    func(t *testing.T, f *fakeBackend) func(c *Controller)
		wantRemove bool
	}{
		{
			name: "cap off and nothing owned",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				return func(c *Controller) { c.Set(capOff) }
			},
		},
		{
			name: "leftover found with the cap on",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				f.setQuota(quotaSet(4), true)
				return func(c *Controller) { c.Set(cpuLimit(t, 50)) }
			},
			wantRemove: true,
		},
		{
			name: "leftover found, then the inspection fails",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				f.setQuota(quotaSet(4), true)
				var failing atomic.Bool
				f.onInspect = func(context.Context) (Inspection, error) {
					if failing.Load() {
						return Inspection{}, errBackend
					}
					return f.model(), nil
				}
				return func(c *Controller) {
					c.Set(cpuLimit(t, 50))
					synctest.Wait()
					failing.Store(true)
					c.Set(cpuLimit(t, 50))
				}
			},
			wantRemove: true,
		},
		{
			name: "unenforced leftover that Remove does not clear",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				f.setQuota(quotaNone, true)
				f.setUnenforced()
				f.onRemove = func(context.Context) error { return nil }
				return func(c *Controller) { c.Set(capOff) }
			},
			wantRemove: true,
		},
		{
			name: "applied, then removed",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				return func(c *Controller) {
					c.Set(cpuLimit(t, 50))
					synctest.Wait()
					c.Set(capOff)
				}
			},
		},
		{
			name: "leftover removed, then the verification is unsupported",
			prepare: func(t *testing.T, f *fakeBackend) func(c *Controller) {
				f.setQuota(quotaSet(2), true)
				inspects := 0
				f.onInspect = func(context.Context) (Inspection, error) {
					inspects++
					if inspects == 2 {
						return Inspection{}, errNoSystemd
					}
					return f.model(), nil
				}
				return func(c *Controller) { c.Set(capOff) }
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				steps := tc.prepare(t, f)
				c, stop := start(t, f)

				steps(c)
				synctest.Wait()
				f.takeCalls()
				stop()
				if tc.wantRemove {
					require.Equal(t, []string{"remove"}, f.takeCalls())
				} else {
					require.Empty(t, f.takeCalls())
				}
			})
		})
	}
}

func TestControllerExitRemovalFailure(t *testing.T) {
	cases := []struct {
		name     string
		remove   func(ctx context.Context) error
		wantTime time.Duration
	}{
		{name: "Remove fails", remove: func(context.Context) error { return errBackend }},
		{
			name: "Remove hangs",
			remove: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantTime: shutdownRemoveTimeout,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFakeBackend(hostCPUs)
				f.onRemove = tc.remove
				c, stop := start(t, f)

				c.Set(cpuLimit(t, 50))
				synctest.Wait()
				f.takeCalls()
				begin := time.Now()
				// stop fails the test if Run returns an error.
				stop()
				require.Equal(t, tc.wantTime, time.Since(begin))
				require.Equal(t, []string{"remove"}, f.takeCalls())
			})
		})
	}
}

func TestControllerRechecksWhileApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()
		want := c.Snapshot()
		requireLastUpdate(t, c)

		for range 3 {
			time.Sleep(recheckInterval - time.Millisecond)
			synctest.Wait()
			require.Empty(t, f.takeCalls())
			time.Sleep(time.Millisecond)
			synctest.Wait()
			require.Equal(t, []string{"cpus", "inspect"}, f.takeCalls())
			require.Equal(t, want, c.Snapshot())
			require.Empty(t, c.Updates(), "a recheck that changes nothing publishes nothing")
		}
	})
}

func TestControllerRecheckCorrectsChangedQuota(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		applies := 0
		f.onApply = func(ctx context.Context, cpus float64) error {
			applies++
			if applies == 2 {
				if err := wait(ctx, release); err != nil {
					return err
				}
			}
			f.setQuota(quotaSet(cpus), true)
			return nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()
		f.setQuota(quotaSet(6), false)

		time.Sleep(recheckInterval)
		synctest.Wait()
		require.Equal(t, StatePending, c.Snapshot().State)
		requireLastUpdate(t, c)

		close(release)
		synctest.Wait()
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect"}, f.takeCalls())
		requireSnapshot(t, Snapshot{
			State: StateApplied, Configured: cpuLimit(t, 50), EffectiveCPUs: 4, ActiveQuota: quotaSet(4),
		}, false, c.Snapshot())
		requireLastUpdate(t, c)
	})
}

func TestControllerRecheckDoesNotDropRunningPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFakeBackend(hostCPUs)
		release := make(chan struct{})
		inspects := 0
		f.onInspect = func(ctx context.Context) (Inspection, error) {
			inspects++
			if inspects == 3 {
				if err := wait(ctx, release); err != nil {
					return Inspection{}, err
				}
			}
			return f.model(), nil
		}
		c, _ := start(t, f)

		c.Set(cpuLimit(t, 50))
		synctest.Wait()
		f.takeCalls()
		f.setQuota(quotaSet(6), false)

		time.Sleep(recheckInterval)
		synctest.Wait()
		c.Set(cpuLimit(t, 50))
		close(release)
		synctest.Wait()
		// The recheck corrects the quota, then the Set runs one more check.
		require.Equal(t, []string{"cpus", "inspect", "apply 4.00", "inspect", "cpus", "inspect"}, f.takeCalls())
		require.Equal(t, StateApplied, c.Snapshot().State)
	})
}

func TestControllerRetryBackoffResets(t *testing.T) {
	t.Run("after a success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFakeBackend(hostCPUs)
			var failing atomic.Bool
			failing.Store(true)
			f.onApply = func(_ context.Context, cpus float64) error {
				if failing.Load() {
					return errBackend
				}
				f.setQuota(quotaSet(cpus), true)
				return nil
			}
			c, _ := start(t, f)

			c.Set(cpuLimit(t, 50))
			synctest.Wait()
			f.takeCalls()
			requireApplyAfter(t, f, 5*time.Second)
			requireApplyAfter(t, f, 10*time.Second)
			failing.Store(false)
			requireApplyAfter(t, f, 20*time.Second)
			require.Equal(t, StateApplied, c.Snapshot().State)

			failing.Store(true)
			f.setQuota(quotaSet(6), false)
			requireApplyAfter(t, f, recheckInterval)
			require.Equal(t, StateDegraded, c.Snapshot().State)
			requireApplyAfter(t, f, 5*time.Second)
		})
	})
	t.Run("after a changed request", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFakeBackend(hostCPUs)
			f.onApply = func(context.Context, float64) error { return errBackend }
			c, _ := start(t, f)

			c.Set(cpuLimit(t, 50))
			synctest.Wait()
			f.takeCalls()
			requireApplyAfter(t, f, 5*time.Second)
			requireApplyAfter(t, f, 10*time.Second)

			c.Set(cpuLimit(t, 25))
			synctest.Wait()
			require.Equal(t, 1, countApplies(f.takeCalls()))
			requireApplyAfter(t, f, 5*time.Second)
		})
	})
}

// start creates a controller with b and runs it.
func start(t *testing.T, b Backend) (*Controller, func()) {
	log, _ := loggertest.New(t.Name())
	c := NewController(log, b)
	return c, run(t, c)
}

// run starts c.Run. The returned stop function cancels it and waits for Run to return.
// It also runs when the test ends.
func run(t *testing.T, c *Controller) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			require.NoError(t, <-done)
		})
	}
	t.Cleanup(stop)
	return stop
}

func cpuLimit(t *testing.T, percent int) limits.CPULimit {
	t.Helper()
	var l limits.CPULimit
	require.NoError(t, l.Unpack(float64(percent)))
	return l
}

// wait blocks until ch is closed or ctx is done.
func wait(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func countApplies(calls []string) int {
	return len(slices.DeleteFunc(calls, func(call string) bool { return !strings.HasPrefix(call, "apply ") }))
}

// requireApplyAfter checks that the next apply runs exactly d from now.
func requireApplyAfter(t *testing.T, f *fakeBackend, d time.Duration) {
	t.Helper()
	time.Sleep(d - time.Millisecond)
	synctest.Wait()
	require.Zero(t, countApplies(f.takeCalls()), "no apply before %v", d)
	time.Sleep(time.Millisecond)
	synctest.Wait()
	require.Equal(t, 1, countApplies(f.takeCalls()), "one apply after %v", d)
}

// requireLastUpdate checks that Updates holds the current Snapshot and nothing else.
func requireLastUpdate(t *testing.T, c *Controller) {
	t.Helper()
	select {
	case s := <-c.Updates():
		require.Equal(t, c.Snapshot(), s)
	default:
		require.Fail(t, "no snapshot on Updates")
	}
}

// requireSnapshot compares got with want. It checks only whether Err is set, not its text.
func requireSnapshot(t *testing.T, want Snapshot, wantErr bool, got Snapshot) {
	t.Helper()
	if wantErr {
		require.Error(t, got.Err)
	} else {
		require.NoError(t, got.Err)
	}
	want.Err, got.Err = nil, nil
	require.Equal(t, want, got)
}
