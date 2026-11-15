// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

// redriveCapFixture builds the publisher under test and its capturing producer.
// It lives here rather than in result_publisher_test.go because that file is at
// the revive file-length limit — the same reason result_publisher_log_tail_test.go
// exists.
type redriveCapFixture struct {
	ctx      context.Context
	producer *capturingSyncProducer
	pub      pkg.ResultPublisher
}

func newRedriveCapFixture() *redriveCapFixture {
	currentDateTime := libtime.NewCurrentDateTime()
	currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))
	producer := &capturingSyncProducer{}
	return &redriveCapFixture{
		ctx:      context.Background(),
		producer: producer,
		pub: pkg.NewResultPublisher(
			producer,
			base.TopicPrefix("develop"),
			currentDateTime,
			nil,
			nil,
		),
	}
}

var _ = Describe("PublishParkRedriveCap", func() {
	It(
		"escalates to status hold and names the non-done result, never closing the record",
		func() {
			// The cap-exhausted park is the escalation for a run that did NOT
			// finish, so it must stay open: writing completed here would report
			// failed work as finished. It must equally not stay in_progress with a
			// terminal phase — the write this test replaced did exactly that (phase
			// human_review, status untouched), leaving 53 update-go rows in_progress
			// with no Job and no re-drive. The assertion is therefore on the triple,
			// not on a single key.
			f := newRedriveCapFixture()
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-redrive-cap"),
				Frontmatter: lib.TaskFrontmatter{
					"status":             "in_progress",
					"phase":              "execution",
					"assignee":           "claude",
					"current_job":        "job-1",
					"target_vault":       "work",
					"last_result_status": "failed",
					"trigger_count":      3,
				},
			}
			Expect(f.pub.PublishParkRedriveCap(f.ctx, task)).To(Succeed())

			Expect(f.producer.messages).To(HaveLen(1))
			_, cmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])

			Expect(cmd.Updates).To(HaveLen(2))
			Expect(cmd.Updates["status"]).To(Equal("hold"))
			// vault-cli's lint flags hold beside an active phase, so the phase has
			// to leave human_review in the same write.
			Expect(cmd.Updates["phase"]).To(Equal("planning"))
			Expect(cmd.Updates).NotTo(HaveKey("assignee"))
			Expect(cmd.Updates).NotTo(HaveKey("current_job"))

			Expect(cmd.Body).NotTo(BeNil())
			Expect(cmd.Body.Heading).To(Equal("## Parked"))
			Expect(cmd.Body.Section).To(ContainSubstring("parked_redrive_cap"))
			Expect(cmd.Body.Section).To(ContainSubstring("last_result_status=failed"))
			Expect(cmd.Body.Section).To(ContainSubstring("phase=execution"))
			Expect(cmd.Body.Section).To(ContainSubstring("trigger_count=3"))
			// The succeeded park's bare token is what made a failed run's section
			// byte-identical to a finished one's. It must not appear alone here.
			Expect(
				cmd.Body.Section,
			).NotTo(Equal("## Parked\n\n- **Reason:** parked_succeeded\n"))
			Expect(cmd.Validate(f.ctx)).To(Succeed())
		},
	)

	It(
		"records an absent result status as absent rather than reading it as done",
		func() {
			// An older agent, or a record written before the field existed, omits
			// last_result_status. Parking is the irreversible direction, so the park
			// must say the evidence was absent rather than invent one.
			f := newRedriveCapFixture()
			task := lib.Task{
				TaskIdentifier: lib.TaskIdentifier("test-task-redrive-cap-absent"),
				Frontmatter: lib.TaskFrontmatter{
					"status":        "in_progress",
					"phase":         "planning",
					"target_vault":  "work",
					"trigger_count": 3,
				},
			}
			Expect(f.pub.PublishParkRedriveCap(f.ctx, task)).To(Succeed())

			Expect(f.producer.messages).To(HaveLen(1))
			_, cmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])

			Expect(cmd.Updates["status"]).To(Equal("hold"))
			Expect(cmd.Body).NotTo(BeNil())
			Expect(cmd.Body.Section).To(ContainSubstring("last_result_status=absent"))
			Expect(cmd.Validate(f.ctx)).To(Succeed())
		},
	)
})
