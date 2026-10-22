// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg"
)

// This file is `package main` (internal) and declares specs without a
// RunSpecs entry point of its own. That is deliberate, and the specs DO run:
// Ginkgo keeps its suite registry per *process*, not per Go test package, so
// the single RunSpecs in main_test.go (package main_test) discovers the specs
// declared here. Verified: `go test -v . -args -ginkgo.v` lists these specs by
// name and reports them in the "Ran N of N" count.
//
// Adding a main_suite_test.go here would be a bug, not a fix — a second
// RunSpecs in the same binary would re-run the whole suite.

// TestParseClaimModeFromEnv is a plain Go test rather than a Ginkgo spec: the
// specs declared in this internal test package are not discovered by the
// package-external suite, so behaviour that must actually run is tested here.
func TestParseClaimModeFromEnv(t *testing.T) {
	t.Run("unset variable is off", func(t *testing.T) {
		os.Unsetenv(claimModeEnv)
		if _, present := os.LookupEnv(claimModeEnv); present {
			t.Fatalf("expected %s to be unset", claimModeEnv)
		}
		mode, err := parseClaimModeFromEnv(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mode != pkg.ClaimModeOff {
			t.Fatalf("expected %q, got %q", pkg.ClaimModeOff, mode)
		}
	})

	t.Run("explicitly empty variable is invalid", func(t *testing.T) {
		os.Setenv(claimModeEnv, "")
		t.Cleanup(func() { os.Unsetenv(claimModeEnv) })
		_, err := parseClaimModeFromEnv(context.Background())
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "off|shadow|on") {
			t.Fatalf("expected accepted set in message, got %q", err.Error())
		}
	})

	t.Run("uppercase ON is invalid", func(t *testing.T) {
		os.Setenv(claimModeEnv, "ON")
		t.Cleanup(func() { os.Unsetenv(claimModeEnv) })
		_, err := parseClaimModeFromEnv(context.Background())
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "ON") {
			t.Fatalf("expected rejected value in message, got %q", err.Error())
		}
	})
}

var _ = Describe("application struct field guards", func() {
	Describe("VaultName field", func() {
		It("declares the env and required tags", func() {
			typ := reflect.TypeOf(application{})
			f, ok := typ.FieldByName("VaultName")
			Expect(ok).To(BeTrue())
			Expect(f.Type.Kind()).To(Equal(reflect.String))
			Expect(f.Tag.Get("env")).To(Equal("VAULT_NAME"))
			Expect(f.Tag.Get("required")).To(Equal("true"))
		})
	})

	Describe("BuildGitVersion field", func() {
		It("declares the env, arg, and default tags", func() {
			typ := reflect.TypeOf(application{})
			f, ok := typ.FieldByName("BuildGitVersion")
			Expect(ok).To(BeTrue())
			Expect(f.Type.Kind()).To(Equal(reflect.String))
			Expect(f.Tag.Get("env")).To(Equal("BUILD_GIT_VERSION"))
			Expect(f.Tag.Get("arg")).To(Equal("build-git-version"))
			Expect(f.Tag.Get("default")).To(Equal("dev"))
		})
	})

	Describe("BuildGitVersion field order", func() {
		It("appears before BuildGitCommit", func() {
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
			Expect(versionIdx).To(BeNumerically(">=", 0))
			Expect(commitIdx).To(BeNumerically(">=", 0))
			Expect(versionIdx).To(BeNumerically("<", commitIdx))
		})
	})
})

var _ = Describe("application.Run startup validation", func() {
	Describe("empty TaskGlob", func() {
		It("fails before the k8s client is constructed, naming the glob setting", func() {
			a := &application{VaultName: "personal", TaskGlob: ""}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			// Off-cluster rest.InClusterConfig() fails too, so asserting only
			// HaveOccurred() would pass whichever check ran first. Naming the
			// glob setting proves the guard precedes the k8s client.
			Expect(err.Error()).To(ContainSubstring("task-glob"))
		})

		It("declares no built-in default on the flag", func() {
			typ := reflect.TypeOf(application{})
			f, ok := typ.FieldByName("TaskGlob")
			Expect(ok).To(BeTrue())
			Expect(f.Tag.Get("env")).To(Equal("TASK_GLOB"))
			Expect(f.Tag.Get("arg")).To(Equal("task-glob"))
			Expect(f.Tag.Get("default")).To(BeEmpty())
		})
	})

	Describe("reconcile timing", func() {
		It("declares env, arg, and the 45s/60s defaults", func() {
			typ := reflect.TypeOf(application{})
			f, ok := typ.FieldByName("ReconcilePassTimeout")
			Expect(ok).To(BeTrue())
			Expect(f.Type).To(Equal(reflect.TypeOf(time.Duration(0))))
			Expect(f.Tag.Get("env")).To(Equal("RECONCILE_PASS_TIMEOUT"))
			Expect(f.Tag.Get("arg")).To(Equal("reconcile-pass-timeout"))
			Expect(f.Tag.Get("default")).To(Equal("45s"))
			f, ok = typ.FieldByName("ReconcileInterval")
			Expect(ok).To(BeTrue())
			Expect(f.Type).To(Equal(reflect.TypeOf(time.Duration(0))))
			Expect(f.Tag.Get("env")).To(Equal("RECONCILE_INTERVAL"))
			Expect(f.Tag.Get("arg")).To(Equal("reconcile-interval"))
			Expect(f.Tag.Get("default")).To(Equal("60s"))
		})

		It("rejects a non-positive pass timeout before the k8s client", func() {
			a := &application{
				VaultName:         "personal",
				TaskGlob:          "t/*.md",
				ReconcileInterval: time.Minute,
			}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reconcile-pass-timeout"))
		})

		It("rejects a non-positive interval before the k8s client", func() {
			a := &application{
				VaultName:            "personal",
				TaskGlob:             "t/*.md",
				ReconcilePassTimeout: time.Minute,
			}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reconcile-interval"))
		})
	})
})
