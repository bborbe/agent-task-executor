// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package spawner

import (
	"testing"
	"time"

	lib "github.com/bborbe/agent"
	batchv1 "k8s.io/api/batch/v1"

	agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"
	pkg "github.com/bborbe/agent-task-executor/pkg"
)

// berlinClock builds the instant in Europe/Berlin rather than converting from
// UTC, so the boundary assertions test the wall-clock time the window is
// declared in.
func berlinClock(t *testing.T, hour, minute int) time.Time {
	t.Helper()
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatalf("load Europe/Berlin: %v", err)
	}
	return time.Date(2026, time.November, 17, hour, minute, 0, 0, location)
}

func factoryWindow() *agentv1.RunWindow {
	return &agentv1.RunWindow{
		Timezone:      "Europe/Berlin",
		WeekdayStart:  "20:00",
		WeekdayEnd:    "06:00",
		WeekendAllDay: true,
	}
}

func TestApplyActiveDeadlineSecondsCapsAtWindowClose(t *testing.T) {
	job := &batchv1.Job{}
	// zombieJobTimeoutSeconds mirrors values-dev.yaml's 10800, so the budget is
	// the stricter of the two and the window is the only variable under test.
	zombie := int32(10800)
	config := pkg.AgentConfiguration{
		RunWindow:               factoryWindow(),
		RunBudgetSeconds:        7200,
		ZombieJobTimeoutSeconds: &zombie,
	}

	applyActiveDeadlineSeconds(config, "job", lib.TaskIdentifier("task"), job, berlinClock(t, 5, 0))

	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("ActiveDeadlineSeconds was not stamped")
	}
	// One hour of window remains, which is stricter than the 7200s budget.
	if got := *job.Spec.ActiveDeadlineSeconds; got != 3600 {
		t.Fatalf("ActiveDeadlineSeconds = %d, want 3600 (window close wins over the budget)", got)
	}
}

func TestApplyActiveDeadlineSecondsKeepsBudgetWellInsideTheWindow(t *testing.T) {
	job := &batchv1.Job{}
	zombie := int32(10800)
	config := pkg.AgentConfiguration{
		RunWindow:               factoryWindow(),
		RunBudgetSeconds:        600,
		ZombieJobTimeoutSeconds: &zombie,
	}

	applyActiveDeadlineSeconds(config, "job", lib.TaskIdentifier("task"), job, berlinClock(t, 21, 0))

	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("ActiveDeadlineSeconds was not stamped")
	}
	if got := *job.Spec.ActiveDeadlineSeconds; got != 600 {
		t.Fatalf("ActiveDeadlineSeconds = %d, want 600 (the budget is stricter here)", got)
	}
}

func TestApplyActiveDeadlineSecondsIgnoresAnAbsentWindow(t *testing.T) {
	job := &batchv1.Job{}
	zombie := int32(10800)
	config := pkg.AgentConfiguration{
		RunBudgetSeconds:        7200,
		ZombieJobTimeoutSeconds: &zombie,
	}

	applyActiveDeadlineSeconds(config, "job", lib.TaskIdentifier("task"), job, berlinClock(t, 5, 0))

	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatal("ActiveDeadlineSeconds was not stamped")
	}
	if got := *job.Spec.ActiveDeadlineSeconds; got != 7200 {
		t.Fatalf("ActiveDeadlineSeconds = %d, want 7200 (no window means no cap)", got)
	}
}
