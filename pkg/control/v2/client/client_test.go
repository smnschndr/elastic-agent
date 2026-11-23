// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/elastic-agent/pkg/control/v2/cproto"
)

func TestToStateCPULimit(t *testing.T) {
	testcases := []struct {
		name     string
		cpuLimit *cproto.CPULimit
		expected *CPULimitState
	}{
		{
			name: "degraded with error",
			cpuLimit: &cproto.CPULimit{
				State:             "degraded",
				ConfiguredPercent: 50,
				EffectiveCpus:     4,
				ActiveQuota:       "set",
				ActiveQuotaCpus:   2.5,
				Error:             "set failed",
			},
			expected: &CPULimitState{
				State:             "degraded",
				ConfiguredPercent: 50,
				EffectiveCPUs:     4,
				ActiveQuota:       "set",
				ActiveQuotaCPUs:   2.5,
				Error:             "set failed",
			},
		},
		{
			name: "unsupported with reason",
			cpuLimit: &cproto.CPULimit{
				State:             "unsupported",
				ConfiguredPercent: 25,
				EffectiveCpus:     1.5,
				ActiveQuota:       "unknown",
				Reason:            "not supported here",
			},
			expected: &CPULimitState{
				State:             "unsupported",
				ConfiguredPercent: 25,
				EffectiveCPUs:     1.5,
				ActiveQuota:       "unknown",
				Reason:            "not supported here",
			},
		},
		{
			name:     "response from an agent without the field",
			cpuLimit: nil,
			expected: nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := toState(&cproto.StateResponse{Info: &cproto.StateAgentInfo{}, CpuLimit: tc.cpuLimit})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, s.CPULimit)
		})
	}
}
