// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
)

var _ = Describe("ResultPublisher park reason", func() {
	var (
		ctx             context.Context
		publisher       pkg.ResultPublisher
		currentDateTime libtime.CurrentDateTime
		producer        *capturingSyncProducer
	)

	BeforeEach(func() {
		ctx = context.Background()
		currentDateTime = libtime.NewCurrentDateTime()
		currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))
		producer = &capturingSyncProducer{}
		publisher = pkg.NewResultPublisher(
			producer,
			base.TopicPrefix("develop"),
			currentDateTime,
			nil,
			nil,
		)
	})

	Describe("PublishParkLostClaim", func() {
		It(
			"writes the phase key and a ## Parked section naming the reason, PR claim key and holder",
			func() {
				// The claim key is derived here, from the same fixture body, and
				// asserted by value: a literal typed into the test would not catch
				// the publisher rendering a different key than the caller derived.
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-park-pr"),
					Frontmatter: lib.TaskFrontmatter{
						"task_type":    "pr-review",
						"repo":         "bborbe/agent",
						"target_vault": "work",
					},
					Content: lib.TaskContent("https://github.com/bborbe/agent/pull/42\n"),
				}
				claimKey, _, fallback, err := claim.DeriveClaimKeysWithBody(
					ctx,
					"pr-review",
					string(task.TaskIdentifier),
					"bborbe",
					"agent",
					string(task.Content),
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(fallback).To(BeFalse())
				Expect(claimKey).To(Equal("claims/pr/bborbe/agent/42"))

				err = publisher.PublishParkLostClaim(
					ctx, task, "parked_lost_claim", claimKey, "previous-task",
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(cmd.Updates).To(HaveLen(1))
				Expect(cmd.Updates["phase"]).To(Equal("human_review"))

				Expect(cmd.Body).NotTo(BeNil())
				Expect(cmd.Body.Heading).To(Equal("## Parked"))
				Expect(cmd.Body.Section).To(HavePrefix("## Parked"))
				Expect(cmd.Body.Section).To(HaveSuffix("\n"))
				Expect(cmd.Body.Section).To(ContainSubstring("parked_lost_claim"))
				Expect(cmd.Body.Section).To(ContainSubstring(claimKey))
				Expect(cmd.Body.Section).To(ContainSubstring("previous-task"))
				Expect(strings.Count(cmd.Body.Section, "\n")).To(BeNumerically("<=", 8))
				Expect(cmd.Validate(ctx)).To(Succeed())
			},
		)

		It(
			"writes a task-id fallback key and unavailable when the takeover created the claim",
			func() {
				task := lib.Task{
					TaskIdentifier: lib.TaskIdentifier("tid-park-fallback"),
					Frontmatter: lib.TaskFrontmatter{
						"task_type": "pr-review",
						"repo":      "bborbe/agent",
					},
					Content: lib.TaskContent("no pull request link here\n"),
				}
				claimKey, _, fallback, err := claim.DeriveClaimKeysWithBody(
					ctx,
					"pr-review",
					string(task.TaskIdentifier),
					"bborbe",
					"agent",
					string(task.Content),
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(fallback).To(BeTrue())
				Expect(claimKey).To(Equal("claims/task/tid-park-fallback"))

				err = publisher.PublishParkLostClaim(
					ctx, task, "parked_lost_claim", claimKey, "",
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.messages).To(HaveLen(1))
				_, cmd := decodeUpdateFrontmatterCommand(producer.messages[0])
				Expect(cmd.Body).NotTo(BeNil())
				Expect(cmd.Body.Heading).To(Equal("## Parked"))
				Expect(cmd.Body.Section).To(ContainSubstring(claimKey))
				Expect(cmd.Body.Section).To(ContainSubstring("unavailable"))
				Expect(strings.Count(cmd.Body.Section, "\n")).To(BeNumerically("<=", 8))
				Expect(cmd.Validate(ctx)).To(Succeed())
			},
		)
	})
})
