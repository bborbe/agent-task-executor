// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	lib "github.com/bborbe/agent"
)

// TaskPhaseReader reads a task's latest published state. It is the fresh-read
// counterpart to TaskStore: the store's entry is written once, on job spawn,
// and never refreshed by a task event, so its phase is always the spawn phase.
// A guard that consults the store cannot tell a task the agent deliberately
// parked from one the agent finished, and would rewrite human_review over
// whatever the agent actually wrote — including done.
//
// It is declared here rather than taken as handler.TaskView because pkg/handler
// already imports pkg, so the reverse edge would be an import cycle. The
// concrete implementation is handler.TaskView, wired in pkg/factory.
//
// No //counterfeiter:generate directive, deliberately. The tests drive this
// interface through mocks.FakeTaskView, which is generated from
// handler.TaskView — a different type that must satisfy this interface
// structurally. The drift the directive exists to prevent (a hand-written fake
// silently missing a method while tests keep passing) cannot happen here: if
// this interface gains a method FakeTaskView lacks, the assignment stops
// compiling. Generating a second fake for the same two methods would add a
// duplicate contract that no test consumes.
type TaskPhaseReader interface {
	// Ready reports whether the reader has caught up with the source it
	// replays. An unready reader cannot prove anything about a task, so its
	// answers must not be acted on.
	Ready() bool
	// Get returns the task for id, or false when the reader holds no entry
	// for it.
	Get(id lib.TaskIdentifier) (lib.Task, bool)
}
