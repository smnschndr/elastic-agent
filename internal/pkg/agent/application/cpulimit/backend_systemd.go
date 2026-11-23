// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build linux

package cpulimit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// The reasons are sent to Fleet, so they must stay short fixed texts without host details.
const (
	reasonNoSystemd       = "not running under systemd"
	reasonCgroupV1        = "cgroup v1 is not supported"
	reasonNoCPUController = "the cpu cgroup controller is not available"
	reasonNotRoot         = "requires root"
	reasonNotService      = "not running as the Elastic Agent service"
)

const (
	// quotaInfinity is the CPUQuotaPerSecUSec value of a unit without a CPU quota.
	quotaInfinity = math.MaxUint64
	// kernelTolerance is the largest accepted difference in CPUs between the unit property and cpu.max.
	// systemd rounds the kernel value down to whole microseconds and uses a longer period for very small quotas.
	kernelTolerance = 0.01
	// quotaDropIn is the name of the drop-in file that systemd writes for CPUQuotaPerSecUSec.
	quotaDropIn = "50-CPUQuota.conf"
)

// systemRuntimeDirs hold the runtime drop-ins that the system manager writes for SetUnitProperties with runtime set:
// the first for units with a unit file, the second for transient units.
var systemRuntimeDirs = []string{"/run/systemd/system.control", "/run/systemd/transient"}

// errUnitNotFound reports that systemd does not know the unit.
var errUnitNotFound = errors.New("the unit is not known to systemd")

// unitInfo holds the systemd properties of a service unit that the backend reads.
type unitInfo struct {
	// id is the primary name of the unit, also when the backend asks by an alias name.
	id           string
	loadState    string
	controlGroup string
	// cpuQuota is CPUQuotaPerSecUSec: CPU time in microseconds per second, or quotaInfinity.
	cpuQuota uint64
	// dropInPaths are the drop-in files that systemd loaded for the unit.
	dropInPaths []string
}

// systemdConn is the part of the systemd D-Bus API that the backend uses.
type systemdConn interface {
	// unit returns an error that wraps errUnitNotFound when systemd does not know the unit.
	unit(ctx context.Context, name string) (unitInfo, error)
	// setCPUQuota sets CPUQuotaPerSecUSec until the next reboot.
	setCPUQuota(ctx context.Context, name string, usec uint64) error
	// reload loads all unit files again, like systemctl daemon-reload. It returns when the reload is done.
	reload(ctx context.Context) error
	close()
}

// systemdBackend sets the runtime CPUQuota of the agent's systemd service. It needs root and cgroup v2.
// The quota is owned when it comes from the runtime override <runtime dir>/<unit>.d/50-CPUQuota.conf, which the agent
// writes, or an administrator with set-property --runtime. Remove deletes that file and reloads systemd, so a static
// CPUQuota from the unit file or a drop-in applies again. The backend never changes other files.
// The reload is the same as systemctl daemon-reload: it loads every unit file again but does not restart units.
//
// Only the agent process is checked to be in the unit cgroup. Command runtime components, such as Beats and the EDOT
// collector, are its child processes and share the cgroup. Service runtime components, such as Endpoint, run in their
// own service and are outside the budget by design.
//
// Known limits:
//   - While the cap is on, the agent's runtime value replaces a static quota of an administrator, also a lower one.
//   - A static quota in a drop-in that sorts after 50-CPUQuota.conf, for example override.conf, or a persistent file
//     with the same name, such as /etc/systemd/system.control/<unit>.d/50-CPUQuota.conf, wins again after the next
//     daemon-reload of the host. Nothing polls for it.
//   - After elastic-agent unprivileged, a downgrade, a rollback or an uninstall, the runtime override stays until
//     the next reboot.
//   - A kernel without CPU bandwidth control never enforces the quota. It is reported as unenforced and retried.
//   - Filesystem calls do not honour the context deadline.
type systemdBackend struct {
	unit string
	// root is the prefix for /proc, /sys and /run paths.
	root string
	// runtimeDirs are the directories of the runtime overrides, such as systemRuntimeDirs.
	runtimeDirs []string
	euid        func() int
	connect     func(ctx context.Context) (systemdConn, error)
}

func (b *systemdBackend) path(p string) string {
	return filepath.Join(b.root, p)
}

func (b *systemdBackend) readFile(ctx context.Context, p string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(b.path(p))
}

// overrideState tells what is on disk for the runtime override of the unit quota.
type overrideState int

const (
	// overrideNone means there is no runtime override, or it is not a regular file, or its path has a symbolic link.
	// Such a file is never deleted.
	overrideNone overrideState = iota
	// overrideFile means the runtime override is a regular file.
	overrideFile
	// overridePending means systemd lists the runtime override but the file is gone. systemd keeps its quota in
	// memory until the next reload.
	overridePending
)

// runtimeOverride finds the runtime override <runtime dir>/<unit id>.d/50-CPUQuota.conf of the unit quota. The quota
// is owned when the state is not overrideNone. Inspect and Remove both use it, so Remove changes nothing that Inspect
// reports as not owned. DropInPaths is used only to find a deleted override that systemd still has in memory: a
// persistent file with the same name hides the runtime one from it.
func (b *systemdBackend) runtimeOverride(ctx context.Context, info unitInfo) (string, overrideState, error) {
	// An empty override over a static quota is not owned.
	if info.cpuQuota == quotaInfinity {
		return "", overrideNone, nil
	}
	pending := ""
	for _, dir := range b.runtimeDirs {
		if err := ctx.Err(); err != nil {
			return "", overrideNone, err
		}
		p := path.Join(dir, info.id+".d", quotaDropIn)
		mode, err := b.lstat(p)
		switch {
		case errors.Is(err, unix.ENOENT):
			if pending == "" && slices.Contains(info.dropInPaths, p) {
				pending = p
			}
		case errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR):
			// The path has a symbolic link or a file where a directory belongs.
		case err != nil:
			return "", overrideNone, fmt.Errorf("failed to check the runtime CPU quota file: %w", err)
		case mode&unix.S_IFMT == unix.S_IFREG:
			return p, overrideFile, nil
		}
	}
	if pending != "" {
		return pending, overridePending, nil
	}
	return "", overrideNone, nil
}

// openDir opens the directory dir below the root. It opens each part of dir with O_NOFOLLOW, so it fails with ELOOP
// or ENOTDIR on a symbolic link.
func (b *systemdBackend) openDir(dir string) (int, error) {
	fd, err := unix.Open(b.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for part := range strings.SplitSeq(strings.Trim(dir, "/"), "/") {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if err != nil {
			return -1, err
		}
		fd = next
	}
	return fd, nil
}

// lstat returns the file mode of p below the root without following a symbolic link anywhere in p.
func (b *systemdBackend) lstat(p string) (uint32, error) {
	fd, err := b.openDir(path.Dir(p))
	if err != nil {
		return 0, err
	}
	defer func() { _ = unix.Close(fd) }()
	var st unix.Stat_t
	if err := unix.Fstatat(fd, path.Base(p), &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0, err
	}
	return st.Mode, nil
}

// removeNoFollow deletes the file p below the root. A symbolic link anywhere in p, also one created after the check,
// cannot redirect the delete: the directories are opened without following links, and unlinkat does not follow one.
func (b *systemdBackend) removeNoFollow(p string) error {
	fd, err := b.openDir(path.Dir(p))
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Unlinkat(fd, path.Base(p), 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return nil
}

func (b *systemdBackend) LogicalCPUs(ctx context.Context) (int, error) {
	if err := b.checkHost(ctx); err != nil {
		return 0, err
	}
	data, err := b.readFile(ctx, "/sys/devices/system/cpu/online")
	if err != nil {
		return 0, err
	}
	return parseCPUList(string(data))
}

// parseCPUList counts the CPUs in a kernel CPU list such as "0-3,5,8-11".
func parseCPUList(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty CPU list")
	}
	count := 0
	for part := range strings.SplitSeq(s, ",") {
		first, last, isRange := strings.Cut(part, "-")
		if !isRange {
			last = first
		}
		lo, err1 := strconv.Atoi(first)
		hi, err2 := strconv.Atoi(last)
		if err1 != nil || err2 != nil || lo < 0 || hi < lo {
			return 0, fmt.Errorf("invalid CPU list %q", s)
		}
		count += hi - lo + 1
	}
	return count, nil
}

// Inspect reports the unit quota and checks it against the kernel. An owned quota that the kernel does not enforce is
// reported as unenforced, so the controller writes or removes it. A quota that is not owned and that the kernel does
// not enforce is reported as no quota, so the controller writes its own when the cap is on.
func (b *systemdBackend) Inspect(ctx context.Context) (Inspection, error) {
	conn, info, err := b.check(ctx)
	if err != nil {
		return Inspection{}, err
	}
	conn.close()

	if info.cpuQuota == quotaInfinity {
		return Inspection{Quota: Quota{Kind: QuotaNone}}, nil
	}
	quota := Quota{Kind: QuotaSet, CPUs: float64(info.cpuQuota) / 1e6}
	_, state, err := b.runtimeOverride(ctx, info)
	if err != nil {
		return Inspection{}, err
	}
	if state == overridePending {
		return Inspection{Owned: true, Unenforced: true}, nil
	}
	enforced, err := b.kernelEnforces(ctx, info.controlGroup, quota.CPUs)
	if err != nil {
		return Inspection{}, err
	}
	switch {
	case state == overrideNone && enforced:
		return Inspection{Quota: quota}, nil
	case state == overrideNone:
		return Inspection{Quota: Quota{Kind: QuotaNone}}, nil
	case enforced:
		return Inspection{Quota: quota, Owned: true}, nil
	default:
		return Inspection{Owned: true, Unenforced: true}, nil
	}
}

// kernelEnforces reports whether cpu.max of the cgroup holds a quota of cpus CPUs. cpu.max does not exist when the
// cpu controller is not enabled for the cgroup or the kernel has no CPU bandwidth control.
func (b *systemdBackend) kernelEnforces(ctx context.Context, controlGroup string, cpus float64) (bool, error) {
	data, err := b.readFile(ctx, filepath.Join("/sys/fs/cgroup", controlGroup, "cpu.max"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read the kernel CPU quota: %w", err)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return false, fmt.Errorf("invalid cpu.max content %q", data)
	}
	if fields[0] == "max" {
		return false, nil
	}
	quota, err1 := strconv.ParseUint(fields[0], 10, 64)
	period, err2 := strconv.ParseUint(fields[1], 10, 64)
	if err1 != nil || err2 != nil || period == 0 {
		return false, fmt.Errorf("invalid cpu.max content %q", data)
	}
	return math.Abs(float64(quota)/float64(period)-cpus) <= kernelTolerance, nil
}

func (b *systemdBackend) Apply(ctx context.Context, cpus float64) error {
	// Whole percent of one CPU keeps the value exact in the systemd runtime override and in cpu.max.
	// The rounding error is at most 0.005 CPUs, below the controller tolerance.
	percent := math.Round(cpus * 100)
	if !(percent >= 1 && percent <= math.MaxUint64/10000) {
		return fmt.Errorf("invalid CPU quota of %v CPUs", cpus)
	}
	conn, _, err := b.check(ctx)
	if err != nil {
		return err
	}
	defer conn.close()
	return conn.setCPUQuota(ctx, b.unit, uint64(percent)*10000)
}

func (b *systemdBackend) Remove(ctx context.Context) error {
	conn, info, err := b.check(ctx)
	if err != nil {
		return err
	}
	defer conn.close()

	p, state, err := b.runtimeOverride(ctx, info)
	if err != nil || state == overrideNone {
		return err
	}
	// A pending override is already deleted. Then only the reload is left.
	if state == overrideFile {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.removeNoFollow(p); err != nil {
			return fmt.Errorf("failed to delete the runtime CPU quota: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := conn.reload(ctx); err != nil {
		return fmt.Errorf("failed to reload systemd: %w", err)
	}
	return nil
}

// checkHost returns an error that wraps *UnsupportedError when the host has no systemd, cgroup v2 or cpu controller.
func (b *systemdBackend) checkHost(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(b.path("/run/systemd/system"))
	if errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()) {
		return &UnsupportedError{Reason: reasonNoSystemd}
	}
	if err != nil {
		return fmt.Errorf("failed to check for systemd: %w", err)
	}
	// Only cgroup v2 has cgroup.controllers in the root of the cgroup file system.
	controllers, err := b.readFile(ctx, "/sys/fs/cgroup/cgroup.controllers")
	if errors.Is(err, fs.ErrNotExist) {
		return &UnsupportedError{Reason: reasonCgroupV1}
	}
	if err != nil {
		return fmt.Errorf("failed to read the cgroup controllers: %w", err)
	}
	if !slices.Contains(strings.Fields(string(controllers)), "cpu") {
		return &UnsupportedError{Reason: reasonNoCPUController}
	}
	return nil
}

// check returns an error that wraps *UnsupportedError when the host or the process is not supported.
// On success it returns an open connection and the unit properties. The caller must close the connection.
func (b *systemdBackend) check(ctx context.Context) (systemdConn, unitInfo, error) {
	if err := b.checkHost(ctx); err != nil {
		return nil, unitInfo{}, err
	}
	// The systemd private socket accepts only root.
	if b.euid() != 0 {
		return nil, unitInfo{}, &UnsupportedError{Reason: reasonNotRoot}
	}
	own, err := b.readFile(ctx, "/proc/self/cgroup")
	if err != nil {
		return nil, unitInfo{}, fmt.Errorf("failed to read the process cgroup: %w", err)
	}

	// checkHost found /run/systemd/system, so a failed connection is unexpected.
	conn, err := b.connect(ctx)
	if err != nil {
		return nil, unitInfo{}, fmt.Errorf("failed to connect to systemd: %w", err)
	}
	info, err := conn.unit(ctx, b.unit)
	if errors.Is(err, errUnitNotFound) || (err == nil && info.loadState == "not-found") {
		conn.close()
		return nil, unitInfo{}, &UnsupportedError{Reason: reasonNotService}
	}
	if err != nil {
		conn.close()
		return nil, unitInfo{}, fmt.Errorf("failed to read the unit properties: %w", err)
	}
	if info.loadState != "loaded" {
		conn.close()
		return nil, unitInfo{}, fmt.Errorf("the unit load state is %q", info.loadState)
	}
	if info.controlGroup == "" || !inCgroup(own, info.controlGroup) {
		conn.close()
		return nil, unitInfo{}, &UnsupportedError{Reason: reasonNotService}
	}
	return conn, info, nil
}

// inCgroup reports whether the cgroup v2 entry in procCgroup, the content of /proc/<pid>/cgroup,
// is the cgroup group or below it.
func inCgroup(procCgroup []byte, group string) bool {
	scanner := bufio.NewScanner(bytes.NewReader(procCgroup))
	for scanner.Scan() {
		if p, ok := strings.CutPrefix(scanner.Text(), "0::"); ok {
			return p == group || strings.HasPrefix(p, group+"/")
		}
	}
	return false
}
