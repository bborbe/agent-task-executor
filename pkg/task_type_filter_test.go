// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	pkg "github.com/bborbe/agent/task/executor/pkg"
)

var _ = Describe("EffectiveTaskTypes", func() {
	It("includes singular taskType when non-empty", func() {
		result := pkg.EffectiveTaskTypes("pr-review", nil)
		Expect(result).To(Equal([]string{"pr-review"}))
	})

	It("includes all taskTypes list elements", func() {
		result := pkg.EffectiveTaskTypes("", []string{"pr-review", "healthcheck"})
		Expect(result).To(Equal([]string{"pr-review", "healthcheck"}))
	})

	It("unions singular and list, singular first", func() {
		result := pkg.EffectiveTaskTypes("pr-review", []string{"healthcheck"})
		Expect(result).To(Equal([]string{"pr-review", "healthcheck"}))
	})

	It("deduplicates when singular appears in list", func() {
		result := pkg.EffectiveTaskTypes("pr-review", []string{"pr-review", "healthcheck"})
		Expect(result).To(Equal([]string{"pr-review", "healthcheck"}))
	})

	It("returns nil when both are empty", func() {
		result := pkg.EffectiveTaskTypes("", nil)
		Expect(result).To(BeNil())
	})

	It("skips empty singular taskType (empty string is not in result)", func() {
		result := pkg.EffectiveTaskTypes("", []string{"pr-review"})
		Expect(result).To(Equal([]string{"pr-review"}))
	})
})

var _ = Describe("TaskTypeInSet", func() {
	It("returns true when taskType is in the set", func() {
		Expect(pkg.TaskTypeInSet("pr-review", []string{"pr-review", "healthcheck"})).To(BeTrue())
	})

	It("returns false when taskType is not in the set", func() {
		Expect(pkg.TaskTypeInSet("code-review", []string{"pr-review", "healthcheck"})).To(BeFalse())
	})

	It("returns false for empty taskType regardless of set", func() {
		Expect(pkg.TaskTypeInSet("", []string{"pr-review"})).To(BeFalse())
	})

	It("returns false for empty taskType against empty set", func() {
		Expect(pkg.TaskTypeInSet("", nil)).To(BeFalse())
	})

	It("returns false when set is empty", func() {
		Expect(pkg.TaskTypeInSet("pr-review", nil)).To(BeFalse())
	})
})
