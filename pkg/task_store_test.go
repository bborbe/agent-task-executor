// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"time"

	lib "github.com/bborbe/agent"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

var _ = Describe("TaskStore", func() {
	var store *pkg.TaskStore

	BeforeEach(func() {
		store = pkg.NewTaskStore()
	})

	It("returns false for unknown identifier", func() {
		_, ok := store.Load(lib.TaskIdentifier("unknown"))
		Expect(ok).To(BeFalse())
	})

	It("stores and loads a task", func() {
		task := lib.Task{TaskIdentifier: lib.TaskIdentifier("tid-1")}
		store.Store(lib.TaskIdentifier("tid-1"), task)
		loaded, ok := store.Load(lib.TaskIdentifier("tid-1"))
		Expect(ok).To(BeTrue())
		Expect(loaded.TaskIdentifier).To(Equal(lib.TaskIdentifier("tid-1")))
	})

	It("deletes a stored task", func() {
		task := lib.Task{TaskIdentifier: lib.TaskIdentifier("tid-2")}
		store.Store(lib.TaskIdentifier("tid-2"), task)
		store.Delete(lib.TaskIdentifier("tid-2"))
		_, ok := store.Load(lib.TaskIdentifier("tid-2"))
		Expect(ok).To(BeFalse())
	})

	It("delete is no-op for unknown identifier", func() {
		Expect(func() { store.Delete(lib.TaskIdentifier("nonexistent")) }).NotTo(Panic())
	})

	It("overwrites existing entry on second Store", func() {
		task1 := lib.Task{
			TaskIdentifier: lib.TaskIdentifier("tid-3"),
			Content:        lib.TaskContent("first"),
		}
		task2 := lib.Task{
			TaskIdentifier: lib.TaskIdentifier("tid-3"),
			Content:        lib.TaskContent("second"),
		}
		store.Store(lib.TaskIdentifier("tid-3"), task1)
		store.Store(lib.TaskIdentifier("tid-3"), task2)
		loaded, ok := store.Load(lib.TaskIdentifier("tid-3"))
		Expect(ok).To(BeTrue())
		Expect(loaded.Content).To(Equal(lib.TaskContent("second")))
	})

	It("records the latest success and ignores an older re-delivery", func() {
		id := lib.TaskIdentifier("tid-s")
		_, ok := store.LastSucceededAt(id)
		Expect(ok).To(BeFalse())
		later := time.Date(2026, 10, 21, 5, 56, 0, 0, time.UTC)
		earlier := later.Add(-time.Minute)
		store.MarkSucceeded(id, later)
		store.MarkSucceeded(id, earlier)
		at, ok := store.LastSucceededAt(id)
		Expect(ok).To(BeTrue())
		Expect(at).To(Equal(later))
	})

	It("keeps the success record after the task entry is deleted", func() {
		id := lib.TaskIdentifier("tid-k")
		store.Store(id, lib.Task{TaskIdentifier: id})
		store.MarkSucceeded(id, time.Now())
		store.Delete(id)
		_, ok := store.LastSucceededAt(id)
		Expect(ok).To(BeTrue())
	})
})
