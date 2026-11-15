// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package factory

import (
	"testing"

	libkafka "github.com/bborbe/kafka"
)

// TestTaskViewConsumerOptions pins the escape hatch's default: the task-view
// reader skips corrupt batches only when the flag says so, because a skipped
// batch is lost data.
//
// This is the reader the SKIP_CRC_CORRUPTION flag exists for. The crash it
// prevents is a CRC-decode failure on a compacted topic (retention.ms=-1) that
// this non-group simple consumer re-reads from offset 0 on every restart, so no
// consumer-group offset reset can clear it.
func TestTaskViewConsumerOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip bool
		want bool
	}{
		{name: "default keeps corrupt batches fatal", skip: false, want: false},
		{name: "flag turns the skip on", skip: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := libkafka.ConsumerOptions{}
			for _, option := range taskViewConsumerOptions(tc.skip) {
				option(&options)
			}
			if options.SkipCorruptBatches != tc.want {
				t.Fatalf(
					"skipCorruptBatches=%v produced SkipCorruptBatches=%v, want %v",
					tc.skip,
					options.SkipCorruptBatches,
					tc.want,
				)
			}
		})
	}
}
