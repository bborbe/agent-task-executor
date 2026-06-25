// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

// ZombieReason is the closed set of machine-readable reason strings emitted in
// the ## Failure body section. Operators grep on these values to triage.
// Adding a new value requires updating this list and the documentation; renaming
// or removing a value is a breaking change to the on-disk task body contract.
type ZombieReason string

const (
	ZombieReasonImagePullBackOff  ZombieReason = "image_pull_backoff"
	ZombieReasonPodEvicted        ZombieReason = "pod_evicted"
	ZombieReasonDeadlineExceeded  ZombieReason = "deadline_exceeded"
	ZombieReasonPodNotScheduled   ZombieReason = "pod_not_scheduled"
	ZombieReasonPodCrashNoStdout  ZombieReason = "pod_crash_no_stdout"
	ZombieReasonExecutorWatchLost ZombieReason = "executor_watch_lost"
	ZombieReasonTypeMismatch      ZombieReason = "type_mismatch"
)

// String returns the reason as a string (for use with PublishFailure).
func (r ZombieReason) String() string { return string(r) }
