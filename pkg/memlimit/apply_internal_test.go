// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memlimit

import (
	stderrors "errors"
	"testing"
)

// fakeFS serves cgroup file contents by path and records what was read.
type fakeFS struct {
	files map[string]string
	errs  map[string]error
	read  []string
}

func (f *fakeFS) readFile(path string) ([]byte, error) {
	f.read = append(f.read, path)
	if err, ok := f.errs[path]; ok {
		return nil, err
	}
	if content, ok := f.files[path]; ok {
		return []byte(content), nil
	}
	return nil, stderrors.New("no such file")
}

// runApply drives apply with a fake filesystem and returns what it recorded.
func runApply(
	env string,
	files map[string]string,
	errs map[string]error,
	paths []string,
) (*fakeFS, []int64) {
	fs := &fakeFS{files: files, errs: errs}
	var got []int64
	apply(env, paths, fs.readFile, func(soft int64) { got = append(got, soft) })
	return fs, got
}

// assertApply checks the limit handed to the runtime and the paths read.
func assertApply(
	t *testing.T,
	fs *fakeFS,
	got []int64,
	wantSet bool,
	wantLimit int64,
	wantRead []string,
) {
	t.Helper()
	if wantSet {
		if len(got) != 1 || got[0] != wantLimit {
			t.Fatalf("SetMemoryLimit calls = %v, want [%d]", got, wantLimit)
		}
	} else if len(got) != 0 {
		t.Fatalf("SetMemoryLimit calls = %v, want none", got)
	}
	if len(fs.read) != len(wantRead) {
		t.Fatalf("read %v, want %v", fs.read, wantRead)
	}
	for i := range wantRead {
		if fs.read[i] != wantRead[i] {
			t.Fatalf("read %v, want %v", fs.read, wantRead)
		}
	}
}

// TestApply covers the branches ApplyFromCgroup cannot reach on a machine whose
// real cgroup filesystem is not the one under test: the GOMEMLIMIT override,
// the v2-before-v1 preference, the unlimited/missing fallbacks, and the value
// handed to debug.SetMemoryLimit.
func TestApply(t *testing.T) {
	const (
		v2 = "/sys/fs/cgroup/memory.max"
		v1 = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	)
	paths := []string{v2, v1}
	perm := stderrors.New("permission denied")

	tests := []struct {
		name      string
		env       string
		files     map[string]string
		errs      map[string]error
		wantSet   bool
		wantLimit int64
		wantRead  []string
	}{
		{
			name:     "GOMEMLIMIT set wins and no file is read",
			env:      "200MiB",
			files:    map[string]string{v2: "268435456"},
			wantRead: nil,
		},
		{
			name:      "cgroup v2 limit applied at 85 percent",
			files:     map[string]string{v2: "268435456"},
			wantSet:   true,
			wantLimit: 228170137,
			wantRead:  []string{v2},
		},
		{
			name:      "v2 missing falls through to v1",
			files:     map[string]string{v1: "536870912"},
			wantSet:   true,
			wantLimit: 456340275,
			wantRead:  []string{v2, v1},
		},
		{
			name:      "v2 unlimited falls through to v1",
			files:     map[string]string{v2: "max", v1: "268435456"},
			wantSet:   true,
			wantLimit: 228170137,
			wantRead:  []string{v2, v1},
		},
		{
			name:     "both unlimited leaves the runtime default",
			files:    map[string]string{v2: "max", v1: "9223372036854771712"},
			wantRead: []string{v2, v1},
		},
		{
			name:     "both unreadable leaves the runtime default",
			errs:     map[string]error{v2: perm, v1: perm},
			wantRead: []string{v2, v1},
		},
		{
			name:      "a read error on v2 does not stop v1",
			errs:      map[string]error{v2: perm},
			files:     map[string]string{v1: "268435456"},
			wantSet:   true,
			wantLimit: 228170137,
			wantRead:  []string{v2, v1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, got := runApply(tt.env, tt.files, tt.errs, paths)
			assertApply(t, fs, got, tt.wantSet, tt.wantLimit, tt.wantRead)
		})
	}
}
