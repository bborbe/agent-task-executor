// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg_test

import (
	"context"
	stderrors "errors"
	"strings"

	lib "github.com/bborbe/agent"
	"github.com/bborbe/cqrs/base"
	libtime "github.com/bborbe/time"
	libtimetest "github.com/bborbe/time/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bborbe/agent-task-executor/mocks"
	"github.com/bborbe/agent-task-executor/pkg"
)

// logTailFixture builds the publisher under test, its capturing producer, the
// fake reader and the pod fixture the log tail is read from. It lives here
// rather than in result_publisher_test.go because that file is at the revive
// file-length limit.
type logTailFixture struct {
	producer *capturingSyncProducer
	reader   *mocks.FakePodLogReader
	pub      pkg.ResultPublisher
	task     lib.Task
	pod      *corev1.Pod
}

func newLogTailFixture() *logTailFixture {
	currentDateTime := libtime.NewCurrentDateTime()
	currentDateTime.SetNow(libtimetest.ParseDateTime("2026-04-18T12:00:00Z"))

	f := &logTailFixture{
		producer: &capturingSyncProducer{},
		reader:   &mocks.FakePodLogReader{},
		task: lib.Task{
			TaskIdentifier: lib.TaskIdentifier("test-task-log-tail"),
			Frontmatter:    lib.TaskFrontmatter{"max_triggers": 3},
		},
		pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "agent", Name: "job-1-abcde"},
		},
	}
	f.pub = pkg.NewResultPublisher(
		f.producer,
		base.TopicPrefix("develop"),
		currentDateTime,
		nil,
		f.reader,
	)
	return f
}

var _ = Describe("PublishFailure redaction in the composed body", func() {
	var f *logTailFixture

	BeforeEach(func() {
		f = newLogTailFixture()
	})

	It("redacts every credential shape from the composed failure body", func() {
		f.reader.TailReturns(
			"2026-11-03T10:00:00Z INFO starting worker ref=abc123\n"+
				"pushed with ghp_0123456789abcdefghijABCDEFGHIJ012345\n"+
				"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln\n"+
				"clone https://deploy:hunter2hunter2@git.example.com/repo.git\n"+
				"ANTHROPIC_AUTH_TOKEN=zz99887766554\n"+
				"-----BEGIN RSA PRIVATE KEY-----\n"+
				"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j\n"+
				"-----END RSA PRIVATE KEY-----\n",
			nil,
		)

		Expect(
			f.pub.PublishFailure(
				context.Background(), f.task, "job-1", "pod_crash_no_stdout", f.pod,
			),
		).To(Succeed())

		_, updateCmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		section := updateCmd.Body.Section
		for _, literal := range []string{
			"ghp_0123456789abcdefghijABCDEFGHIJ012345",
			"BEGIN RSA PRIVATE KEY",
			"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j",
			"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln",
			"hunter2hunter2",
			"zz99887766554",
		} {
			Expect(section).NotTo(ContainSubstring(literal))
		}
		Expect(section).To(ContainSubstring("<redacted>"))
	})

	It("keeps a short lookup key readable in the composed body", func() {
		f.reader.TailReturns("2026-11-03T10:00:00Z INFO lookup ref=abc123\n", nil)

		Expect(
			f.pub.PublishFailure(
				context.Background(), f.task, "job-1", "pod_crash_no_stdout", f.pod,
			),
		).To(Succeed())

		_, updateCmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		Expect(updateCmd.Body.Section).To(ContainSubstring("ref=abc123"))
	})

	It("composes the tail as one fenced block", func() {
		f.reader.TailReturns("line one\nline two\n", nil)

		Expect(
			f.pub.PublishFailure(
				context.Background(), f.task, "job-1", "pod_crash_no_stdout", f.pod,
			),
		).To(Succeed())

		_, updateCmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		Expect(updateCmd.Body.Section).To(ContainSubstring(
			"- **Log tail:**\n\n```\nline one\nline two\n```\n",
		))
		Expect(strings.Count(updateCmd.Body.Section, "```")).To(Equal(2))
	})
})

var _ = Describe("PublishFailure log tail read failures", func() {
	var f *logTailFixture

	BeforeEach(func() {
		f = newLogTailFixture()
	})

	It("log read fails: still publishes the failure without a log block", func() {
		f.reader.TailReturns("", stderrors.New(`pods "job-1-abcde" not found`))

		var err error
		out := captureGlogStderr(func() {
			err = f.pub.PublishFailure(
				context.Background(), f.task, "job-1", "pod_crash_no_stdout", f.pod,
			)
		})

		Expect(err).To(BeNil())
		_, updateCmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		Expect(updateCmd.Body.Section).To(ContainSubstring("## Failure"))
		Expect(updateCmd.Body.Section).NotTo(ContainSubstring("Log tail"))
		Expect(strings.Count(out, "event=log_tail_unavailable ")).To(Equal(1))
		Expect(out).To(ContainSubstring("job=job-1"))
	})

	It("reads no log on the success path", func() {
		Expect(
			f.pub.PublishClearCurrentJob(context.Background(), f.task, "job-success"),
		).To(Succeed())

		Expect(f.reader.TailCallCount()).To(Equal(0))
		Expect(f.producer.messages).To(HaveLen(1))
		_, cmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		Expect(cmd.Body).To(BeNil())
	})

	It("writes no log block when the failure carries no pod", func() {
		Expect(
			f.pub.PublishFailure(
				context.Background(), f.task, "job-nopod", "deadline_exceeded", nil,
			),
		).To(Succeed())

		Expect(f.reader.TailCallCount()).To(Equal(0))
		_, updateCmd := decodeUpdateFrontmatterCommand(f.producer.messages[0])
		Expect(updateCmd.Body.Section).NotTo(ContainSubstring("Log tail"))
	})
})
