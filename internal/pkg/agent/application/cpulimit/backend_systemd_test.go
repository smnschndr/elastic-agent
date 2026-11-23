// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build linux

package cpulimit

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-agent/pkg/core/logger/loggertest"
)

const (
	testUnit       = "elastic-agent.service"
	testCgroup     = "/system.slice/elastic-agent.service"
	testCPUMax     = "/sys/fs/cgroup" + testCgroup + "/cpu.max"
	testDropIn     = "/run/systemd/system.control/" + testUnit + ".d/50-CPUQuota.conf"
	testTransient  = "/run/systemd/transient/" + testUnit + ".d/50-CPUQuota.conf"
	testPersistent = "/etc/systemd/system.control/" + testUnit + ".d/50-CPUQuota.conf"
	testStatic     = "/etc/systemd/system/" + testUnit + ".d/override.conf"
)

// fakeDropInDirs returns the drop-in directories of the unit id, from the highest priority to the lowest.
func fakeDropInDirs(id string) []string {
	return []string{
		"/etc/systemd/system.control/" + id + ".d",
		"/run/systemd/system.control/" + id + ".d",
		"/run/systemd/transient/" + id + ".d",
		"/etc/systemd/system/" + id + ".d",
	}
}

// fakeConn models systemd. The unit file has no quota.
//
// A quota write creates the runtime override <runtime dir>/<unit id>.d/50-CPUQuota.conf, lists it and changes the quota
// and cpu.max at once, also when a persistent file with the same name exists.
//
// A reload reads the *.conf files of fakeDropInDirs again: the listed drop-ins, the quota and cpu.max follow the files.
// Of two files with the same name only the one in the directory with the higher priority counts. A symbolic link is
// followed and a dangling one is skipped. Only CPUQuota= lines in the [Service] section count, and the last one in
// file-name order wins. cpu.max does not exist without a quota, because systemd then turns the cpu controller off.
type fakeConn struct {
	t         *testing.T
	b         *systemdBackend
	info      unitInfo
	unitErr   error
	setErr    error
	reloadErr error
	// transient is true for a transient unit, whose runtime override is in /run/systemd/transient.
	transient bool
	// onUnit runs at the start of each unit read.
	onUnit func()

	connects int
	closes   int
	reloads  int
	sets     []uint64
}

func (f *fakeConn) unit(_ context.Context, name string) (unitInfo, error) {
	if f.onUnit != nil {
		f.onUnit()
	}
	if name != testUnit {
		return unitInfo{}, errors.New("unknown unit")
	}
	info := f.info
	info.dropInPaths = slices.Clone(f.info.dropInPaths)
	return info, f.unitErr
}

func (f *fakeConn) setCPUQuota(_ context.Context, name string, usec uint64) error {
	if f.setErr != nil {
		return f.setErr
	}
	if name != testUnit {
		return errors.New("unknown unit")
	}
	f.sets = append(f.sets, usec)
	dir := "/run/systemd/system.control"
	if f.transient {
		dir = "/run/systemd/transient"
	}
	p := dir + "/" + f.info.id + ".d/50-CPUQuota.conf"
	writeFile(f.t, f.b, p, fmt.Sprintf("[Service]\nCPUQuota=%.2f%%\n", float64(usec)/10000))
	if !slices.Contains(f.info.dropInPaths, p) {
		f.info.dropInPaths = append(f.info.dropInPaths, p)
	}
	f.setLive(usec)
	return nil
}

func (f *fakeConn) reload(ctx context.Context) error {
	f.reloads++
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.reloadErr != nil {
		return f.reloadErr
	}
	byName := map[string]string{}
	for _, dir := range fakeDropInDirs(f.info.id) {
		entries, err := os.ReadDir(f.b.path(dir))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		require.NoError(f.t, err)
		for _, e := range entries {
			if _, ok := byName[e.Name()]; ok || !strings.HasSuffix(e.Name(), ".conf") {
				continue
			}
			fi, err := os.Stat(f.b.path(dir + "/" + e.Name()))
			if errors.Is(err, fs.ErrNotExist) || (err == nil && fi.IsDir()) {
				continue
			}
			require.NoError(f.t, err)
			byName[e.Name()] = dir + "/" + e.Name()
		}
	}
	f.info.dropInPaths = nil
	usec := uint64(quotaInfinity)
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		p := byName[name]
		f.info.dropInPaths = append(f.info.dropInPaths, p)
		data, err := os.ReadFile(f.b.path(p))
		require.NoError(f.t, err)
		section := ""
		for line := range strings.Lines(string(data)) {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[") {
				section = line
			} else if v, ok := strings.CutPrefix(line, "CPUQuota="); ok && section == "[Service]" {
				usec = parseQuota(f.t, v)
			}
		}
	}
	f.setLive(usec)
	return nil
}

// parseQuota converts a CPUQuota value such as "50%" to microseconds per second. An empty value means no quota.
func parseQuota(t *testing.T, v string) uint64 {
	if v == "" {
		return quotaInfinity
	}
	percent, err := strconv.ParseFloat(strings.TrimSuffix(v, "%"), 64)
	require.NoError(t, err)
	return uint64(math.Round(percent * 10000))
}

func (f *fakeConn) setLive(usec uint64) {
	f.info.cpuQuota = usec
	if usec == quotaInfinity {
		require.NoError(f.t, os.RemoveAll(f.b.path(testCPUMax)))
		return
	}
	writeFile(f.t, f.b, testCPUMax, fmt.Sprintf("%d 100000\n", usec/10))
}

func (f *fakeConn) close() {
	f.closes++
}

// newTestBackend returns a backend on a fake supported host with 14 CPUs and no CPU quota.
func newTestBackend(t *testing.T) (*systemdBackend, *fakeConn) {
	// The root has no symbolic link, so the backend sees only the links of the test.
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	conn := &fakeConn{
		t:    t,
		info: unitInfo{id: testUnit, loadState: "loaded", controlGroup: testCgroup, cpuQuota: quotaInfinity},
	}
	b := &systemdBackend{
		unit:        testUnit,
		root:        root,
		runtimeDirs: systemRuntimeDirs,
		euid:        func() int { return 0 },
		connect: func(context.Context) (systemdConn, error) {
			conn.connects++
			return conn, nil
		},
	}
	conn.b = b
	require.NoError(t, os.MkdirAll(b.path("/run/systemd/system"), 0o755))
	writeFile(t, b, "/sys/fs/cgroup/cgroup.controllers", "cpuset cpu io memory pids\n")
	writeFile(t, b, "/proc/self/cgroup", "0::"+testCgroup+"\n")
	writeFile(t, b, "/sys/devices/system/cpu/online", "0-13\n")
	return b, conn
}

func writeFile(t *testing.T, b *systemdBackend, path, content string) {
	t.Helper()
	p := b.path(path)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
}

// symlink creates link that points to target, both below the root.
func symlink(t *testing.T, b *systemdBackend, target, link string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(b.path(link)), 0o755))
	require.NoError(t, os.Symlink(b.path(target), b.path(link)))
}

// fileExists reports whether path exists. It does not follow a symbolic link at the end of path.
func fileExists(t *testing.T, b *systemdBackend, path string) bool {
	t.Helper()
	_, err := os.Lstat(b.path(path))
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	require.NoError(t, err)
	return true
}

// setStatic gives the unit a quota from a drop-in that is not the runtime override.
func setStatic(t *testing.T, b *systemdBackend, conn *fakeConn, percent int) {
	t.Helper()
	writeFile(t, b, testStatic, fmt.Sprintf("[Service]\nCPUQuota=%d%%\n", percent))
	require.NoError(t, conn.reload(t.Context()))
	conn.reloads = 0
}

func reasonOf(err error) string {
	var unsupported *UnsupportedError
	if errors.As(err, &unsupported) {
		return unsupported.Reason
	}
	return ""
}

func TestSystemdBackendSupport(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T, b *systemdBackend, conn *fakeConn)
		reason string
		// hostLevel is true when the reason is found without a connection to systemd.
		hostLevel bool
		wantErr   bool
	}{
		{
			name:  "supported",
			setup: func(*testing.T, *systemdBackend, *fakeConn) {},
		},
		{
			name: "the agent is in a cgroup below the unit cgroup",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				writeFile(t, b, "/proc/self/cgroup", "0::"+testCgroup+"/sub\n")
			},
		},
		{
			name: "systemd is not running",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				require.NoError(t, os.Remove(b.path("/run/systemd/system")))
			},
			reason:    reasonNoSystemd,
			hostLevel: true,
		},
		{
			name: "the systemd marker is not a directory",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				require.NoError(t, os.Remove(b.path("/run/systemd/system")))
				writeFile(t, b, "/run/systemd/system", "")
			},
			reason:    reasonNoSystemd,
			hostLevel: true,
		},
		{
			name: "cgroup v1",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				require.NoError(t, os.Remove(b.path("/sys/fs/cgroup/cgroup.controllers")))
			},
			reason:    reasonCgroupV1,
			hostLevel: true,
		},
		{
			name: "no cpu controller",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				writeFile(t, b, "/sys/fs/cgroup/cgroup.controllers", "cpuset io memory pids\n")
			},
			reason:    reasonNoCPUController,
			hostLevel: true,
		},
		{
			name: "not root",
			setup: func(_ *testing.T, b *systemdBackend, _ *fakeConn) {
				b.euid = func() int { return 1000 }
			},
			reason:    reasonNotRoot,
			hostLevel: true,
		},
		{
			name: "the unit is not found",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.info = unitInfo{loadState: "not-found"}
			},
			reason: reasonNotService,
		},
		{
			name: "the unit is masked",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.info = unitInfo{loadState: "masked"}
			},
			wantErr: true,
		},
		{
			name: "the unit failed to load",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.info = unitInfo{loadState: "error"}
			},
			wantErr: true,
		},
		{
			name: "the unit has no cgroup",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.info.controlGroup = ""
			},
			reason: reasonNotService,
		},
		{
			name: "the agent is outside the unit cgroup",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				writeFile(t, b, "/proc/self/cgroup", "0::/user.slice/user-1000.slice/session-2.scope\n")
			},
			reason: reasonNotService,
		},
		{
			name: "the agent is in a cgroup with the same name prefix",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				writeFile(t, b, "/proc/self/cgroup", "0::"+testCgroup+"-other\n")
			},
			reason: reasonNotService,
		},
		{
			name: "the agent cgroup has no cgroup v2 entry",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				writeFile(t, b, "/proc/self/cgroup", "1:name=systemd:"+testCgroup+"\n")
			},
			reason: reasonNotService,
		},
		{
			name: "the systemd socket is missing",
			setup: func(_ *testing.T, b *systemdBackend, _ *fakeConn) {
				b.connect = func(context.Context) (systemdConn, error) {
					return nil, &net.OpError{Op: "dial", Net: "unix", Err: os.NewSyscallError("connect", syscall.ENOENT)}
				}
			},
			wantErr: true,
		},
		{
			name: "the systemd socket refuses the connection",
			setup: func(_ *testing.T, b *systemdBackend, _ *fakeConn) {
				b.connect = func(context.Context) (systemdConn, error) {
					return nil, &net.OpError{Op: "dial", Net: "unix", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
				}
			},
			wantErr: true,
		},
		{
			name: "systemd does not know the unit",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.unitErr = fmt.Errorf("%w: no such unit", errUnitNotFound)
			},
			reason: reasonNotService,
		},
		{
			name: "the cgroup controllers cannot be read",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				require.NoError(t, os.Remove(b.path("/sys/fs/cgroup/cgroup.controllers")))
				writeFile(t, b, "/sys/fs/cgroup/cgroup.controllers/x", "")
			},
			wantErr: true,
		},
		{
			name: "the agent cgroup cannot be read",
			setup: func(t *testing.T, b *systemdBackend, _ *fakeConn) {
				require.NoError(t, os.Remove(b.path("/proc/self/cgroup")))
				writeFile(t, b, "/proc/self/cgroup/x", "")
			},
			wantErr: true,
		},
		{
			name: "the connection fails",
			setup: func(_ *testing.T, b *systemdBackend, _ *fakeConn) {
				b.connect = func(context.Context) (systemdConn, error) { return nil, errors.New("no socket") }
			},
			wantErr: true,
		},
		{
			name: "the unit properties cannot be read",
			setup: func(_ *testing.T, _ *systemdBackend, conn *fakeConn) {
				conn.unitErr = errors.New("read failed")
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, conn := newTestBackend(t)
			tc.setup(t, b, conn)

			_, inspectErr := b.Inspect(t.Context())
			applyErr := b.Apply(t.Context(), 2)
			switch {
			case tc.reason != "":
				require.Equal(t, tc.reason, reasonOf(inspectErr))
				require.Equal(t, tc.reason, reasonOf(applyErr))
				require.Empty(t, conn.sets)
				if tc.hostLevel {
					require.Zero(t, conn.connects)
				}
			case tc.wantErr:
				require.Error(t, inspectErr)
				require.Error(t, applyErr)
				require.Empty(t, reasonOf(inspectErr))
				require.Empty(t, reasonOf(applyErr))
				require.Empty(t, conn.sets)
			default:
				require.NoError(t, inspectErr)
				require.NoError(t, applyErr)
			}
			require.Equal(t, conn.connects, conn.closes)
		})
	}
}

func TestSystemdBackendLogicalCPUs(t *testing.T) {
	cases := []struct {
		content string
		exp     int
		wantErr bool
	}{
		{content: "0", exp: 1},
		{content: "0-13\n", exp: 14},
		{content: "0-3,5,8-11\n", exp: 9},
		{content: "2,4", exp: 2},
		{content: "", wantErr: true},
		{content: "\n", wantErr: true},
		{content: "abc", wantErr: true},
		{content: "3-1", wantErr: true},
		{content: "0-", wantErr: true},
		{content: "0,,1", wantErr: true},
		{content: "-1", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.content, func(t *testing.T) {
			b, _ := newTestBackend(t)
			writeFile(t, b, "/sys/devices/system/cpu/online", tc.content)
			n, err := b.LogicalCPUs(t.Context())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.exp, n)
		})
	}

	t.Run("missing file", func(t *testing.T) {
		b, _ := newTestBackend(t)
		require.NoError(t, os.Remove(b.path("/sys/devices/system/cpu/online")))
		_, err := b.LogicalCPUs(t.Context())
		require.Error(t, err)
	})

	t.Run("an unsupported host reports its reason", func(t *testing.T) {
		b, _ := newTestBackend(t)
		require.NoError(t, os.Remove(b.path("/sys/fs/cgroup/cgroup.controllers")))
		require.NoError(t, os.Remove(b.path("/sys/devices/system/cpu/online")))
		_, err := b.LogicalCPUs(t.Context())
		require.Equal(t, reasonCgroupV1, reasonOf(err))
	})

	t.Run("a cancelled context", func(t *testing.T) {
		b, _ := newTestBackend(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := b.LogicalCPUs(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestSystemdBackendInspect(t *testing.T) {
	owned := func(cpus float64) Inspection { return Inspection{Quota: quotaSet(cpus), Owned: true} }
	unenforced := Inspection{Owned: true, Unenforced: true}
	cases := []struct {
		name string
		usec uint64
		// runtime is what the runtime override holds: "" nothing, "file" a regular file, "listed" no file but
		// DropInPaths lists it.
		runtime string
		// path is the runtime override. It is testDropIn when empty.
		path string
		// cpuMax is the content of cpu.max. An empty value means the file does not exist.
		cpuMax  string
		exp     Inspection
		wantErr bool
	}{
		{name: "no quota", usec: quotaInfinity, exp: Inspection{Quota: quotaNone}},
		{name: "no quota and the kernel still has the old one", usec: quotaInfinity, cpuMax: "700000 100000\n", exp: Inspection{Quota: quotaNone}},
		{name: "an empty runtime override", usec: quotaInfinity, runtime: "file", cpuMax: "700000 100000\n", exp: Inspection{Quota: quotaNone}},

		{name: "static quota", usec: 500000, cpuMax: "50000 100000\n", exp: Inspection{Quota: quotaSet(0.5)}},
		{name: "static quota that the kernel rounds down", usec: 1234567, cpuMax: "123456 100000\n", exp: Inspection{Quota: quotaSet(1.234567)}},
		{name: "static quota with another kernel quota", usec: 500000, cpuMax: "350000 100000\n", exp: Inspection{Quota: quotaNone}},
		{name: "static quota without a kernel quota", usec: 500000, cpuMax: "max 100000\n", exp: Inspection{Quota: quotaNone}},
		{name: "static quota without cpu.max", usec: 500000, exp: Inspection{Quota: quotaNone}},
		{name: "static quota with an invalid cpu.max", usec: 500000, cpuMax: "50000\n", wantErr: true},

		{name: "runtime quota", usec: 7000000, runtime: "file", cpuMax: "700000 100000\n", exp: owned(7)},
		{name: "runtime quota of a transient unit", usec: 7000000, runtime: "file", path: testTransient, cpuMax: "700000 100000\n", exp: owned(7)},
		{name: "runtime quota that the kernel rounds down", usec: 1234567, runtime: "file", cpuMax: "123456 100000\n", exp: owned(1.234567)},
		{name: "runtime quota with another period", usec: 50000, runtime: "file", cpuMax: "10000 200000\n", exp: owned(0.05)},
		{name: "runtime quota without a kernel quota", usec: 7000000, runtime: "file", cpuMax: "max 100000\n", exp: unenforced},
		{name: "runtime quota with another kernel quota", usec: 7000000, runtime: "file", cpuMax: "350000 100000\n", exp: unenforced},
		{name: "runtime quota without cpu.max", usec: 7000000, runtime: "file", exp: unenforced},
		{name: "runtime quota with an invalid cpu.max", usec: 7000000, runtime: "file", cpuMax: "700000\n", wantErr: true},
		{name: "runtime quota with a zero period", usec: 7000000, runtime: "file", cpuMax: "700000 0\n", wantErr: true},
		{name: "runtime quota with a cpu.max that is not a number", usec: 7000000, runtime: "file", cpuMax: "a b\n", wantErr: true},

		{name: "runtime override deleted but not reloaded", usec: 7000000, runtime: "listed", cpuMax: "700000 100000\n", exp: unenforced},
		{name: "transient override deleted but not reloaded", usec: 7000000, runtime: "listed", path: testTransient, cpuMax: "700000 100000\n", exp: unenforced},
		{name: "a deleted override that is not listed", usec: 7000000, cpuMax: "700000 100000\n", exp: Inspection{Quota: quotaSet(7)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, conn := newTestBackend(t)
			p := tc.path
			if p == "" {
				p = testDropIn
			}
			switch tc.runtime {
			case "file":
				writeFile(t, b, p, "[Service]\n")
				conn.info.dropInPaths = []string{p}
			case "listed":
				conn.info.dropInPaths = []string{p}
			}
			conn.info.cpuQuota = tc.usec
			if tc.cpuMax != "" {
				writeFile(t, b, testCPUMax, tc.cpuMax)
			}

			ins, err := b.Inspect(t.Context())
			if tc.wantErr {
				require.Error(t, err)
				require.Empty(t, reasonOf(err))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.exp, ins)
		})
	}
}

// TestSystemdBackendOverridePath checks that only the regular file <runtime dir>/<unit id>.d/50-CPUQuota.conf is
// owned, and that Remove deletes exactly what Inspect reports as owned.
func TestSystemdBackendOverridePath(t *testing.T) {
	const quota = "[Service]\nCPUQuota=700%\n"
	listed := func(conn *fakeConn, p string) { conn.info.dropInPaths = append(conn.info.dropInPaths, p) }
	cases := []struct {
		name  string
		setup func(t *testing.T, b *systemdBackend, conn *fakeConn)
		// deleted is the owned override that Remove deletes. It is empty when nothing is owned.
		deleted string
		// kept are the paths that must stay.
		kept []string
	}{
		{
			name: "runtime override",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, testDropIn, quota)
				listed(conn, testDropIn)
			},
			deleted: testDropIn,
		},
		{
			name: "runtime override of a transient unit",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, testTransient, quota)
				listed(conn, testTransient)
			},
			deleted: testTransient,
		},
		{
			name: "the unit is an alias: the override of the unit id",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				conn.info.id = "real.service"
				writeFile(t, b, "/run/systemd/system.control/real.service.d/50-CPUQuota.conf", quota)
			},
			deleted: "/run/systemd/system.control/real.service.d/50-CPUQuota.conf",
		},
		{
			name: "the unit is an alias: a directory of the alias name",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				conn.info.id = "real.service"
				writeFile(t, b, testDropIn, quota)
				listed(conn, testDropIn)
			},
			kept: []string{testDropIn},
		},
		{
			name: "prefix drop-in",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, "/run/systemd/system.control/elastic-.service.d/50-CPUQuota.conf", quota)
				listed(conn, "/run/systemd/system.control/elastic-.service.d/50-CPUQuota.conf")
			},
			kept: []string{"/run/systemd/system.control/elastic-.service.d/50-CPUQuota.conf"},
		},
		{
			name: "type drop-in",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, "/run/systemd/system.control/service.d/50-CPUQuota.conf", quota)
				listed(conn, "/run/systemd/system.control/service.d/50-CPUQuota.conf")
			},
			kept: []string{"/run/systemd/system.control/service.d/50-CPUQuota.conf"},
		},
		{
			name: "persistent override of set-property",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, testPersistent, quota)
				listed(conn, testPersistent)
			},
			kept: []string{testPersistent},
		},
		{
			name: "drop-in with the same name in /etc/systemd/system",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				p := "/etc/systemd/system/" + testUnit + ".d/50-CPUQuota.conf"
				writeFile(t, b, p, quota)
				listed(conn, p)
			},
			kept: []string{"/etc/systemd/system/" + testUnit + ".d/50-CPUQuota.conf"},
		},
		{
			name: "another runtime property",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				p := filepath.Dir(testDropIn) + "/50-MemoryMax.conf"
				writeFile(t, b, p, quota)
				listed(conn, p)
			},
			kept: []string{filepath.Dir(testDropIn) + "/50-MemoryMax.conf"},
		},
		{
			name: "the override is a symbolic link",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, "/var/lib/other.conf", quota)
				symlink(t, b, "/var/lib/other.conf", testDropIn)
				listed(conn, testDropIn)
			},
			kept: []string{testDropIn, "/var/lib/other.conf"},
		},
		{
			name: "the override is a dangling symbolic link",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				symlink(t, b, "/var/lib/missing.conf", testDropIn)
				listed(conn, testDropIn)
			},
			kept: []string{testDropIn},
		},
		{
			name: "the override is a directory",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, testDropIn+"/x", "")
				listed(conn, testDropIn)
			},
			kept: []string{testDropIn + "/x"},
		},
		{
			name: "the unit directory is a symbolic link",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, "/var/lib/other/50-CPUQuota.conf", quota)
				symlink(t, b, "/var/lib/other", filepath.Dir(testDropIn))
				listed(conn, testDropIn)
			},
			kept: []string{"/var/lib/other/50-CPUQuota.conf"},
		},
		{
			name: "the runtime directory is a symbolic link",
			setup: func(t *testing.T, b *systemdBackend, conn *fakeConn) {
				writeFile(t, b, "/var/lib/control/"+testUnit+".d/50-CPUQuota.conf", quota)
				symlink(t, b, "/var/lib/control", "/run/systemd/system.control")
				listed(conn, testDropIn)
			},
			kept: []string{"/var/lib/control/" + testUnit + ".d/50-CPUQuota.conf"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, conn := newTestBackend(t)
			tc.setup(t, b, conn)
			conn.setLive(7000000)

			ins, err := b.Inspect(t.Context())
			require.NoError(t, err)
			require.Equal(t, tc.deleted != "", ins.Owned)

			require.NoError(t, b.Remove(t.Context()))
			if tc.deleted != "" {
				require.Equal(t, 1, conn.reloads)
				require.False(t, fileExists(t, b, tc.deleted))
			} else {
				require.Zero(t, conn.reloads)
			}
			for _, p := range tc.kept {
				require.True(t, fileExists(t, b, p), p)
			}
		})
	}
}

func TestSystemdBackendApply(t *testing.T) {
	cases := []struct {
		name string
		cpus float64
		usec uint64
	}{
		{name: "whole CPUs", cpus: 7, usec: 7000000},
		{name: "rounded up", cpus: 2.636, usec: 2640000},
		{name: "rounded down", cpus: 2.634, usec: 2630000},
		{name: "the smallest floor", cpus: 0.05, usec: 50000},
		{name: "4096 CPUs", cpus: 4096, usec: 4096000000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, conn := newTestBackend(t)
			require.NoError(t, b.Apply(t.Context(), tc.cpus))
			require.Equal(t, []uint64{tc.usec}, conn.sets)
			ins, err := b.Inspect(t.Context())
			require.NoError(t, err)
			require.True(t, ins.Owned)
			require.InDelta(t, float64(tc.usec)/1e6, ins.Quota.CPUs, 1e-9)
		})
	}

	for _, cpus := range []float64{math.NaN(), 0, -1, 0.004, math.Inf(1), 1e300} {
		t.Run(fmt.Sprintf("invalid value %v", cpus), func(t *testing.T) {
			b, conn := newTestBackend(t)
			require.Error(t, b.Apply(t.Context(), cpus))
			require.Empty(t, reasonOf(b.Apply(t.Context(), cpus)))
			require.Zero(t, conn.connects)
		})
	}

	t.Run("a failed write is returned", func(t *testing.T) {
		b, conn := newTestBackend(t)
		writeErr := errors.New("write failed")
		conn.setErr = writeErr
		require.ErrorIs(t, b.Apply(t.Context(), 7), writeErr)
		require.Equal(t, conn.connects, conn.closes)
	})

	t.Run("repeated calls", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		require.NoError(t, b.Apply(t.Context(), 7))
		require.Equal(t, []uint64{7000000, 7000000}, conn.sets)
		require.Zero(t, conn.reloads)
		require.Equal(t, conn.connects, conn.closes)
	})

	t.Run("replaces a static quota", func(t *testing.T) {
		b, conn := newTestBackend(t)
		setStatic(t, b, conn, 50)
		require.NoError(t, b.Apply(t.Context(), 7))
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, quotaSet(7), ins.Quota)
		require.True(t, ins.Owned)
	})
}

func TestSystemdBackendRemove(t *testing.T) {
	t.Run("a static quota is kept", func(t *testing.T) {
		b, conn := newTestBackend(t)
		setStatic(t, b, conn, 50)
		require.NoError(t, b.Remove(t.Context()))
		require.Empty(t, conn.sets)
		require.Zero(t, conn.reloads)
		require.True(t, fileExists(t, b, testStatic))
		require.Equal(t, conn.connects, conn.closes)
	})

	t.Run("deletes the runtime override and reloads", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		writeFile(t, b, filepath.Dir(testDropIn)+"/50-MemoryMax.conf", "[Service]\nMemoryMax=1G\n")

		require.NoError(t, b.Remove(t.Context()))
		require.Equal(t, 1, conn.reloads)
		require.False(t, fileExists(t, b, testDropIn))
		require.True(t, fileExists(t, b, filepath.Dir(testDropIn)+"/50-MemoryMax.conf"))
		require.Equal(t, []uint64{7000000}, conn.sets)
		require.Equal(t, conn.connects, conn.closes)

		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)

		require.NoError(t, b.Remove(t.Context()))
		require.Equal(t, 1, conn.reloads)
	})

	t.Run("the static quota applies again", func(t *testing.T) {
		b, conn := newTestBackend(t)
		setStatic(t, b, conn, 50)
		require.NoError(t, b.Apply(t.Context(), 7))

		require.NoError(t, b.Remove(t.Context()))
		require.True(t, fileExists(t, b, testStatic))
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaSet(0.5)}, ins)
	})

	t.Run("a runtime override of an administrator is removed", func(t *testing.T) {
		b, conn := newTestBackend(t)
		writeFile(t, b, testDropIn, "[Service]\nCPUQuota=200.00%\n")
		require.NoError(t, conn.reload(t.Context()))
		conn.reloads = 0
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaSet(2), Owned: true}, ins)

		require.NoError(t, b.Remove(t.Context()))
		require.False(t, fileExists(t, b, testDropIn))
		require.Equal(t, 1, conn.reloads)
		ins, err = b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})

	t.Run("a persistent file with the same name does not hide the runtime override", func(t *testing.T) {
		b, conn := newTestBackend(t)
		writeFile(t, b, testPersistent, "[Service]\nCPUQuota=30%\n")
		require.NoError(t, b.Apply(t.Context(), 7))
		// A daemon-reload of the host lists only the persistent file.
		require.NoError(t, conn.reload(t.Context()))
		conn.reloads = 0
		require.Equal(t, []string{testPersistent}, conn.info.dropInPaths)

		require.NoError(t, b.Remove(t.Context()))
		require.False(t, fileExists(t, b, testDropIn))
		require.True(t, fileExists(t, b, testPersistent))
		require.Equal(t, 1, conn.reloads)
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaSet(0.3)}, ins)
	})

	t.Run("a listed override without a file only reloads", func(t *testing.T) {
		b, conn := newTestBackend(t)
		conn.info.dropInPaths = []string{testDropIn}
		conn.setLive(7000000)
		require.NoError(t, b.Remove(t.Context()))
		require.Equal(t, 1, conn.reloads)
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})

	t.Run("a failed reload is returned and a retry finishes the removal", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		reloadErr := errors.New("reload failed")
		conn.reloadErr = reloadErr

		require.ErrorIs(t, b.Remove(t.Context()), reloadErr)
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Owned: true, Unenforced: true}, ins)

		conn.reloadErr = nil
		require.NoError(t, b.Remove(t.Context()))
		ins, err = b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})

	t.Run("a cancel after the unit read does not delete", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		ctx, cancel := context.WithCancel(t.Context())
		conn.onUnit = cancel

		require.ErrorIs(t, b.Remove(ctx), context.Canceled)
		require.True(t, fileExists(t, b, testDropIn))
		require.Zero(t, conn.reloads)
		conn.onUnit = nil
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, quotaSet(7), ins.Quota)
	})

	t.Run("a cancel at the reload is finished by the next removal", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		ctx, cancel := context.WithCancel(t.Context())
		b.connect = func(context.Context) (systemdConn, error) {
			conn.connects++
			return cancelOnReload{fakeConn: conn, cancel: cancel}, nil
		}

		require.ErrorIs(t, b.Remove(ctx), context.Canceled)
		require.False(t, fileExists(t, b, testDropIn))

		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Owned: true, Unenforced: true}, ins)

		require.NoError(t, b.Remove(t.Context()))
		ins, err = b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})

	t.Run("an empty runtime override over a static quota is kept", func(t *testing.T) {
		b, conn := newTestBackend(t)
		writeFile(t, b, "/etc/systemd/system/"+testUnit+".d/10-quota.conf", "[Service]\nCPUQuota=50%\n")
		writeFile(t, b, testDropIn, "[Service]\nCPUQuota=\n")
		require.NoError(t, conn.reload(t.Context()))
		conn.reloads = 0
		require.Equal(t, uint64(quotaInfinity), conn.info.cpuQuota)

		require.NoError(t, b.Remove(t.Context()))
		require.Zero(t, conn.reloads)
		require.True(t, fileExists(t, b, testDropIn))
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})

	t.Run("a failed delete does not reload", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can delete in a read-only directory")
		}
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		dir := b.path(filepath.Dir(testDropIn))
		require.NoError(t, os.Chmod(dir, 0o555))
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

		require.Error(t, b.Remove(t.Context()))
		require.Zero(t, conn.reloads)
		require.True(t, fileExists(t, b, testDropIn))
	})

	t.Run("the delete does not follow a symbolic link that appears after the check", func(t *testing.T) {
		b, _ := newTestBackend(t)
		writeFile(t, b, "/var/lib/other/50-CPUQuota.conf", "")
		symlink(t, b, "/var/lib/other", filepath.Dir(testDropIn))
		require.Error(t, b.removeNoFollow(testDropIn))
		require.True(t, fileExists(t, b, "/var/lib/other/50-CPUQuota.conf"))
	})

	t.Run("an unsupported host", func(t *testing.T) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		b.euid = func() int { return 1000 }
		require.Equal(t, reasonNotRoot, reasonOf(b.Remove(t.Context())))
		require.True(t, fileExists(t, b, testDropIn))
		require.Zero(t, conn.reloads)
	})
}

// cancelOnReload cancels the context of the call when the reload starts, like a shutdown at that moment.
type cancelOnReload struct {
	*fakeConn
	cancel context.CancelFunc
}

func (c cancelOnReload) reload(ctx context.Context) error {
	c.cancel()
	return c.fakeConn.reload(ctx)
}

// TestSystemdBackendPendingRemovalAfterRestart covers a removal whose reload failed before the agent restarted:
// systemd still has the quota in memory, but its override is gone.
func TestSystemdBackendPendingRemovalAfterRestart(t *testing.T) {
	restart := func(t *testing.T) (*Controller, *systemdBackend, *fakeConn) {
		b, conn := newTestBackend(t)
		require.NoError(t, b.Apply(t.Context(), 7))
		conn.reloadErr = errors.New("reload failed")
		require.Error(t, b.Remove(t.Context()))
		require.False(t, fileExists(t, b, testDropIn))
		conn.reloadErr = nil
		conn.sets, conn.reloads = nil, 0

		restarted := &systemdBackend{unit: b.unit, root: b.root, runtimeDirs: b.runtimeDirs, euid: b.euid, connect: b.connect}
		log, _ := loggertest.New(t.Name())
		return NewController(log, restarted), restarted, conn
	}

	t.Run("the cap is on with the same target", func(t *testing.T) {
		c, b, conn := restart(t)
		c.Set(cpuLimit(t, 50))
		require.Equal(t, recheckInterval, c.reconcile(t.Context()))
		require.Equal(t, []uint64{7000000}, conn.sets)
		require.True(t, fileExists(t, b, testDropIn))
		require.Equal(t, StateApplied, c.Snapshot().State)
		require.Equal(t, quotaSet(7), c.Snapshot().ActiveQuota)
	})

	t.Run("the cap is off", func(t *testing.T) {
		c, b, conn := restart(t)
		c.Set(capOff)
		require.Zero(t, c.reconcile(t.Context()))
		require.Equal(t, 1, conn.reloads)
		require.Empty(t, conn.sets)
		require.Equal(t, StateDisabled, c.Snapshot().State)
		ins, err := b.Inspect(t.Context())
		require.NoError(t, err)
		require.Equal(t, Inspection{Quota: quotaNone}, ins)
	})
}

// TestSystemdBackendStaticQuotaUnenforced covers a static quota equal to the target that the kernel does not enforce.
func TestSystemdBackendStaticQuotaUnenforced(t *testing.T) {
	b, conn := newTestBackend(t)
	setStatic(t, b, conn, 700)
	require.NoError(t, os.Remove(b.path(testCPUMax)))
	log, _ := loggertest.New(t.Name())
	c := NewController(log, b)

	c.Set(cpuLimit(t, 50))
	require.Equal(t, recheckInterval, c.reconcile(t.Context()))
	require.Equal(t, []uint64{7000000}, conn.sets)
	require.Equal(t, StateApplied, c.Snapshot().State)
}

func TestFakeConnReload(t *testing.T) {
	b, conn := newTestBackend(t)
	dir := "/etc/systemd/system/" + testUnit + ".d"
	writeFile(t, b, dir+"/10-unit.conf", "[Unit]\nCPUQuota=10%\n")
	writeFile(t, b, dir+"/20-off.conf.disabled", "[Service]\nCPUQuota=20%\n")
	symlink(t, b, "/var/lib/missing.conf", dir+"/30-dangling.conf")
	writeFile(t, b, "/var/lib/linked.conf", "[Service]\nCPUQuota=40%\n")
	symlink(t, b, "/var/lib/linked.conf", dir+"/40-linked.conf")
	require.NoError(t, conn.reload(t.Context()))
	require.Equal(t, []string{dir + "/10-unit.conf", dir + "/40-linked.conf"}, conn.info.dropInPaths)
	require.Equal(t, uint64(400000), conn.info.cpuQuota)
	require.True(t, fileExists(t, b, testCPUMax))

	require.NoError(t, os.Remove(b.path(dir+"/40-linked.conf")))
	require.NoError(t, conn.reload(t.Context()))
	require.Equal(t, uint64(quotaInfinity), conn.info.cpuQuota)
	require.False(t, fileExists(t, b, testCPUMax))
}
