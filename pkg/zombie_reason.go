// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import "strings"

// ZombieReason is the closed set of machine-readable reason strings emitted in
// the ## Failure body section. Operators grep on these values to triage.
// OOM kills use the dedicated pod_oom_killed value so operators can grep for
// OOM specifically; other non-zero pod terminations fall back to the generic
// pod_crash_no_stdout. Adding a new value requires updating this list and the
// documentation; renaming or removing a value is a breaking change to the
// on-disk task body contract.
type ZombieReason string

// Every new ZombieReason must be added to this block AND to the
// ParseZombieReason table in zombie_reason_test.go. The per-value String()
// specs do not cover the parser.
const (
	ZombieReasonImagePullBackOff  ZombieReason = "image_pull_backoff"
	ZombieReasonPodEvicted        ZombieReason = "pod_evicted"
	ZombieReasonDeadlineExceeded  ZombieReason = "deadline_exceeded"
	ZombieReasonPodNotScheduled   ZombieReason = "pod_not_scheduled"
	ZombieReasonPodCrashNoStdout  ZombieReason = "pod_crash_no_stdout"
	ZombieReasonExecutorWatchLost ZombieReason = "executor_watch_lost"
	ZombieReasonTypeMismatch      ZombieReason = "type_mismatch"
	ZombieReasonPodOOMKilled      ZombieReason = "pod_oom_killed"
)

// zombieReasonValues is the closed vocabulary as a slice, so ParseZombieReason
// can recognise every value. Every new ZombieReason constant must be added here
// AND to the ParseZombieReason table in zombie_reason_test.go — the per-value
// String() specs do not cover the parser.
var zombieReasonValues = []ZombieReason{
	ZombieReasonImagePullBackOff,
	ZombieReasonPodEvicted,
	ZombieReasonDeadlineExceeded,
	ZombieReasonPodNotScheduled,
	ZombieReasonPodCrashNoStdout,
	ZombieReasonExecutorWatchLost,
	ZombieReasonTypeMismatch,
	ZombieReasonPodOOMKilled,
}

// jobFailedConditionAliases maps the Kubernetes JobFailed condition reasons the
// classifier publishes onto the vocabulary value they classify as. The published
// failure_reason names the Job's own condition, so without these aliases every
// such crash would parse to false and crashFailureUpdates would log
// decision=not_crash_class — silently disarming the escalation this vocabulary
// exists to drive. DeadlineExceeded is deliberately absent: it never reaches the
// fallthrough and is published as its own vocabulary value.
var jobFailedConditionAliases = map[string]ZombieReason{
	"BackoffLimitExceeded": ZombieReasonPodCrashNoStdout,
	"PodFailurePolicy":     ZombieReasonPodCrashNoStdout,
	"JobFailed":            ZombieReasonPodCrashNoStdout,
}

// ParseZombieReason returns the vocabulary value s names, and false when it
// names none. It matches up to the first space, so the formatted form produced
// by backoffFailureReason — "pod_oom_killed (OOMKilled/137)" — parses to
// ZombieReasonPodOOMKilled. A Kubernetes JobFailed condition reason — published
// as "BackoffLimitExceeded (pod-unresolved)" or "JobFailed (Error/137)" — parses
// through jobFailedConditionAliases to the class it belongs to. Anything else,
// including the empty string, returns false.
//
// The formatted string is never persisted: its spaces and parentheses are not
// valid claim key segments. Callers store the parsed value, or "unknown" when
// this returns false.
func ParseZombieReason(s string) (ZombieReason, bool) {
	value := s
	if idx := strings.IndexByte(value, ' '); idx >= 0 {
		value = value[:idx]
	}
	for _, reason := range zombieReasonValues {
		if reason.String() == value {
			return reason, true
		}
	}
	if reason, ok := jobFailedConditionAliases[value]; ok {
		return reason, true
	}
	return "", false
}

// String returns the reason as a string (for use with PublishFailure).
func (r ZombieReason) String() string { return string(r) }
