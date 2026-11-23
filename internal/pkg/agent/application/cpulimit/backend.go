// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package cpulimit

import (
	"context"
)

// Backend changes and reads the CPU quota in the operating system.
// Every method must return when ctx is done.
// A method returns an error that wraps *UnsupportedError when enforcement is unavailable on this host by design.
type Backend interface {
	// LogicalCPUs returns the number of online logical CPUs on the host.
	LogicalCPUs(ctx context.Context) (int, error)
	// Inspect reads the quota on the group that Apply changes.
	// It returns an error when the agent process is outside that group. Command runtime components, such as Beats and
	// the EDOT collector, are its child processes and share the group. Service runtime components, such as Endpoint,
	// run in their own service and are outside the budget by design.
	// It must not fail only because the group is not prepared for a quota.
	Inspect(ctx context.Context) (Inspection, error)
	// Apply sets the agent quota to cpus CPUs.
	Apply(ctx context.Context, cpus float64) error
	// Remove clears the agent quota. It does nothing when the agent has no quota.
	Remove(ctx context.Context) error
}

// Inspection is the result of Backend.Inspect.
type Inspection struct {
	// Quota is the quota on the group. Its Kind is QuotaNone or QuotaSet.
	Quota Quota
	// Owned is true when Quota comes from the override that Remove clears: the agent's own, or on systemd one that an
	// administrator wrote with set-property --runtime. A static quota is never owned and Remove never changes it.
	// Owned must stay true while such an override is in force, also after a failed Remove, because the controller
	// treats an unowned quota as removed.
	Owned bool
	// Unenforced is true when the quota is Owned but the kernel does not enforce it, for example
	// because a reload is pending or the kernel limit is missing. Quota then holds no value.
	Unenforced bool
}

// UnsupportedError reports that enforcement is unavailable on this host by design.
type UnsupportedError struct {
	// Reason explains why, for example "not running under systemd".
	Reason string
}

// Error returns the reason with a short prefix.
func (e *UnsupportedError) Error() string {
	return "CPU limit is not supported: " + e.Reason
}
