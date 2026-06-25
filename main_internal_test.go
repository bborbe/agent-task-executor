// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"reflect"
	"testing"
)

func TestApplicationBuildGitVersionFieldExists(t *testing.T) {
	typ := reflect.TypeOf(application{})
	f, ok := typ.FieldByName("BuildGitVersion")
	if !ok {
		t.Fatalf("application struct is missing BuildGitVersion field")
	}
	if f.Type.Kind() != reflect.String {
		t.Fatalf("BuildGitVersion must be string, got %s", f.Type.Kind())
	}
	if got, want := f.Tag.Get("env"), "BUILD_GIT_VERSION"; got != want {
		t.Errorf("BuildGitVersion env tag = %q, want %q", got, want)
	}
	if got, want := f.Tag.Get("arg"), "build-git-version"; got != want {
		t.Errorf("BuildGitVersion arg tag = %q, want %q", got, want)
	}
	if got, want := f.Tag.Get("default"), "dev"; got != want {
		t.Errorf("BuildGitVersion default tag = %q, want %q", got, want)
	}
}

func TestApplicationBuildGitVersionFieldOrder(t *testing.T) {
	typ := reflect.TypeOf(application{})
	versionIdx, commitIdx := -1, -1
	for i := 0; i < typ.NumField(); i++ {
		switch typ.Field(i).Name {
		case "BuildGitVersion":
			versionIdx = i
		case "BuildGitCommit":
			commitIdx = i
		}
	}
	if versionIdx < 0 || commitIdx < 0 {
		t.Fatalf(
			"both BuildGitVersion (%d) and BuildGitCommit (%d) must exist",
			versionIdx,
			commitIdx,
		)
	}
	if versionIdx >= commitIdx {
		t.Errorf(
			"BuildGitVersion (idx %d) must appear before BuildGitCommit (idx %d)",
			versionIdx,
			commitIdx,
		)
	}
}
