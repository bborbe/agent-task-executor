// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memlimit applies the container's memory ceiling to the Go runtime.
//
// Without it the runtime never learns the cgroup limit: GOGC defaults to 100,
// so the heap may grow to twice the live set before a collection is forced,
// regardless of how much memory the container is actually allowed to use. An
// executor whose live heap is ~108MiB therefore targets ~216MiB and is
// OOM-killed by a 256Mi limit it was never told about.
package memlimit

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// softLimitRatio is the fraction of the container limit handed to the runtime.
// The remainder is headroom for what the soft limit does not cover: goroutine
// stacks, memory mapped outside the heap, and the kernel's own accounting.
// Measured on agent-task-executor-openclaw, RSS sat ~20MiB above heap_inuse
// after a forced GC, so 0.85 leaves margin rather than pinning the runtime to
// the ceiling.
const softLimitRatio = 0.85

// cgroupLimitPaths are the files a container's memory ceiling is published in,
// in preference order: cgroup v2, then cgroup v1.
var cgroupLimitPaths = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// ApplyFromCgroup reads the container's memory limit and applies it to the Go
// runtime as a soft memory limit.
//
// An explicit GOMEMLIMIT wins: the runtime has already applied it, and
// overwriting it here would silently discard an operator's setting. A missing
// or unlimited cgroup file leaves the runtime's own default in place, so the
// function never lowers a limit it could not read.
func ApplyFromCgroup() {
	if env := strings.TrimSpace(os.Getenv("GOMEMLIMIT")); env != "" {
		glog.Infof("event=memory_limit source=env GOMEMLIMIT=%s", env)
		return
	}
	for _, path := range cgroupLimitPaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit, ok := ParseCgroupLimit(string(raw))
		if !ok {
			continue
		}
		soft := int64(float64(limit) * softLimitRatio)
		debug.SetMemoryLimit(soft)
		glog.Infof(
			"event=memory_limit source=%s cgroup_bytes=%d soft_limit_bytes=%d",
			path,
			limit,
			soft,
		)
		return
	}
	glog.Infof("event=memory_limit source=none soft_limit_bytes=unset")
}

// ParseCgroupLimit converts a cgroup memory file's contents into a byte count.
//
// It reports false for the three shapes that do not name a real ceiling:
// cgroup v2's "max", an empty file, and cgroup v1's unlimited sentinel, which
// the kernel publishes instead of omitting the file. A malformed or
// non-positive value is also rejected.
func ParseCgroupLimit(raw string) (int64, bool) {
	value := strings.TrimSpace(raw)
	if value == "" || value == "max" {
		return 0, false
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil || limit <= 0 {
		return 0, false
	}
	// cgroup v1 publishes ~9223372036854771712 when the group is unlimited.
	if limit > 1<<60 {
		return 0, false
	}
	return limit, true
}
