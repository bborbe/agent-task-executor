// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	stderrors "errors"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// logReactor returns a PrependReactor that serves body for a log request and
// declines every other get-pods action. It records the options it was handed,
// which is the only place the fake exposes them: the action carries the options
// in Value, and it carries no pod name at all (GenericActionImpl has none).
func logReactor(
	body string,
	serveErr error,
	captured **corev1.PodLogOptions,
	namespace *string,
) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "log" {
			return false, nil, nil
		}
		if options, ok := action.(k8stesting.GenericAction).GetValue().(*corev1.PodLogOptions); ok {
			*captured = options
		}
		*namespace = action.GetNamespace()
		if serveErr != nil {
			return true, nil, serveErr
		}
		return true, &runtime.Unknown{Raw: []byte(body)}, nil
	}
}

var _ = Describe("podLogReader", func() {
	const namespace = "agent"

	var (
		captured      *corev1.PodLogOptions
		namespaceSeen string
	)

	BeforeEach(func() {
		captured = nil
		namespaceSeen = ""
	})

	It("requests a line-limited and byte-limited tail", func() {
		client := fake.NewClientset()
		client.PrependReactor("get", "pods", logReactor("hello\n", nil, &captured, &namespaceSeen))

		tail, err := NewPodLogReader(client).Tail(context.Background(), namespace, "job-1-abcde")

		Expect(err).To(BeNil())
		Expect(tail).To(Equal("hello\n"))
		Expect(captured).NotTo(BeNil())
		Expect(captured.TailLines).NotTo(BeNil())
		Expect(*captured.TailLines).To(Equal(int64(logTailMaxLines)))
		Expect(captured.LimitBytes).NotTo(BeNil())
		Expect(*captured.LimitBytes).To(Equal(int64(logTailMaxBytes)))
		Expect(namespaceSeen).To(Equal(namespace))
	})

	It("returns at most the byte limit for an over-limit log", func() {
		client := fake.NewClientset()
		client.PrependReactor(
			"get", "pods",
			logReactor(strings.Repeat("x", logTailMaxBytes*10), nil, &captured, &namespaceSeen),
		)

		tail, err := NewPodLogReader(client).Tail(context.Background(), namespace, "job-1-abcde")

		Expect(err).To(BeNil())
		Expect(len(tail)).To(BeNumerically("<=", logTailMaxBytes))
	})

	It("returns the log text unchanged", func() {
		body := "line one\nline two\n"
		client := fake.NewClientset()
		client.PrependReactor("get", "pods", logReactor(body, nil, &captured, &namespaceSeen))

		tail, err := NewPodLogReader(client).Tail(context.Background(), namespace, "job-1-abcde")

		Expect(err).To(BeNil())
		Expect(tail).To(Equal(body))
	})

	It("returns the API error", func() {
		client := fake.NewClientset()
		client.PrependReactor(
			"get",
			"pods",
			logReactor(
				"",
				stderrors.New(`pods "job-1-abcde" not found`),
				&captured,
				&namespaceSeen,
			),
		)

		tail, err := NewPodLogReader(client).Tail(context.Background(), namespace, "job-1-abcde")

		Expect(tail).To(BeEmpty())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(namespace + "/job-1-abcde"))
	})
})
