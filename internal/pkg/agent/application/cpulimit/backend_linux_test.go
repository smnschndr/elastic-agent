// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//go:build linux

package cpulimit

import (
	"errors"
	"testing"

	"github.com/godbus/dbus/v5"
	"github.com/stretchr/testify/require"
)

func TestUnitError(t *testing.T) {
	require.ErrorIs(t, unitError(dbus.Error{Name: "org.freedesktop.systemd1.NoSuchUnit"}), errUnitNotFound)
	require.ErrorIs(t, unitError(dbus.Error{Name: "org.freedesktop.DBus.Error.UnknownObject"}), errUnitNotFound)
	require.NotErrorIs(t, unitError(dbus.Error{Name: "org.freedesktop.DBus.Error.AccessDenied"}), errUnitNotFound)
	require.NotErrorIs(t, unitError(errors.New("timeout")), errUnitNotFound)
}
