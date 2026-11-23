// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package configuration

import "github.com/elastic/elastic-agent/pkg/component"

type InternalConfig struct {
	Runtime *component.RuntimeConfig `yaml:"runtime" config:"runtime" json:"runtime"`
	Limits  InternalLimitsConfig     `yaml:"limits" config:"limits" json:"limits"`
}

// InternalLimitsConfig holds the internal settings of the agent limits.
type InternalLimitsConfig struct {
	CPU InternalCPULimitConfig `yaml:"cpu" config:"cpu" json:"cpu"`
}

// InternalCPULimitConfig holds the internal settings of the CPU cap.
type InternalCPULimitConfig struct {
	// PreviewEnabled turns on the CPU cap preview (agent.internal.limits.cpu.enabled). When it is off,
	// agent.limits.cpu is validated but not enforced. It is read once at startup, so a change needs an Agent restart.
	PreviewEnabled bool `yaml:"enabled" config:"enabled" json:"enabled"`
}

func DefaultInternalConfig() *InternalConfig {
	return &InternalConfig{
		Runtime: component.DefaultRuntimeConfig(),
	}
}
