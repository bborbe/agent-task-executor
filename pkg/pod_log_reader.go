// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	"io"
	"time"

	"github.com/bborbe/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// logTailMaxLines bounds the number of trailing lines requested from the log API.
	logTailMaxLines = 200
	// logTailMaxBytes bounds the bytes requested from the log API and the bytes
	// read from the stream. Both halves are needed: the API limit is what keeps a
	// runaway log out of the response, and the stream limit keeps a response that
	// ignores it from being buffered whole.
	logTailMaxBytes = 64 * 1024
	// logTailReadTimeout bounds one log read. A hung stream must not stall a
	// failure publish: the publish is the record, and a read is only evidence.
	logTailReadTimeout = 10 * time.Second
)

//counterfeiter:generate -o ../mocks/pod_log_reader.go --fake-name FakePodLogReader . PodLogReader

// PodLogReader reads the tail of one pod's log directly from the Kubernetes API.
// It takes a client and a namespace per call and holds no informer cache, so it
// can be constructed independently of the Job watcher.
type PodLogReader interface {
	// Tail returns the pod's trailing log text, bounded by logTailMaxLines and
	// logTailMaxBytes. It returns an error when the API refuses the read — the
	// pod is already garbage-collected, the caller lacks the pods/log grant, or
	// the API throttles — and the caller is expected to swallow that error: a
	// missing tail must never turn a delivered failure into an error.
	Tail(ctx context.Context, namespace string, podName string) (string, error)
}

// podLogReader is the API-backed PodLogReader. It holds only a client: no
// informer, no cache, no namespace of its own.
//
// The line and byte limits are applied AT THE API, through PodLogOptions, so a
// runaway log never reaches this process. The io.LimitReader over the stream is
// the second half of the same bound — not a post-hoc truncation of an
// already-buffered log, which would be too late to help.
type podLogReader struct {
	client kubernetes.Interface
}

// NewPodLogReader creates a PodLogReader backed by client. It reads the log
// subresource directly and therefore needs the pods/log RBAC grant; without it
// every call fails and the caller publishes the failure with no tail.
func NewPodLogReader(client kubernetes.Interface) PodLogReader {
	return &podLogReader{client: client}
}

// Tail implements PodLogReader.
func (r *podLogReader) Tail(
	ctx context.Context,
	namespace string,
	podName string,
) (string, error) {
	tailLines := int64(logTailMaxLines)
	limitBytes := int64(logTailMaxBytes)

	readCtx, cancel := context.WithTimeout(ctx, logTailReadTimeout)
	defer cancel()

	stream, err := r.client.CoreV1().Pods(namespace).
		GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailLines, LimitBytes: &limitBytes}).
		Stream(readCtx)
	if err != nil {
		return "", errors.Wrapf(ctx, err, "stream log for pod %s/%s", namespace, podName)
	}
	defer func() { _ = stream.Close() }()

	body, err := io.ReadAll(io.LimitReader(stream, limitBytes))
	if err != nil {
		return "", errors.Wrapf(ctx, err, "read log for pod %s/%s", namespace, podName)
	}
	return string(body), nil
}
