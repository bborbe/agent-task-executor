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
// unsetClaimModeEnv removes CLAIM_MODE for one test and restores whatever value
// the process held before, so a case cannot leak its environment into the rest
// of the package's tests.
func unsetClaimModeEnv(t *testing.T) {
	t.Helper()
	prior, present := os.LookupEnv(claimModeEnv)
	if err := os.Unsetenv(claimModeEnv); err != nil {
		t.Fatalf("unset %s: %v", claimModeEnv, err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(claimModeEnv, prior)
			return
		}
		_ = os.Unsetenv(claimModeEnv)
	})
}

func TestParseClaimModeFromEnv(t *testing.T) {
	t.Run("unset variable is off", func(t *testing.T) {
		unsetClaimModeEnv(t)
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
		t.Setenv(claimModeEnv, "")
		_, err := parseClaimModeFromEnv(context.Background())
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "off|shadow|on") {
			t.Fatalf("expected accepted set in message, got %q", err.Error())
		}
	})

	t.Run("uppercase ON is invalid", func(t *testing.T) {
		t.Setenv(claimModeEnv, "ON")
		_, err := parseClaimModeFromEnv(context.Background())
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "ON") {
			t.Fatalf("expected rejected value in message, got %q", err.Error())
		}
	})
}

// TestValidateClaimTransport pins the startup gate: off validates nothing
// (it reads no secret and builds no client), while a claim-taking mode refuses
// to start without a git-rest URL rather than failing on its first dispatch.
func TestValidateClaimTransport(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		mode    pkg.ClaimMode
		url     string
		cluster string
		want    string
	}{
		{"off needs no url", pkg.ClaimModeOff, "", "", ""},
		{"off ignores a set url", pkg.ClaimModeOff, "http://git-rest", "self", ""},
		{"shadow requires a url", pkg.ClaimModeShadow, "", "self", "GITREST_URL"},
		{"shadow accepts a url", pkg.ClaimModeShadow, "http://git-rest", "self", ""},
		{"on is refused without a url", pkg.ClaimModeOn, "", "self", "not available yet"},
		{"on is refused with a url", pkg.ClaimModeOn, "http://git-rest", "self", "not available yet"},
		{"zero value needs no url", pkg.ClaimMode(""), "", "", ""},
		{"shadow accepts https", pkg.ClaimModeShadow, "https://git-rest", "self", ""},
		{"shadow rejects a url with no scheme", pkg.ClaimModeShadow, "git-rest", "self", "GITREST_URL"},
		{"shadow rejects a url with no host", pkg.ClaimModeShadow, "http://", "self", "GITREST_URL"},
		{"shadow rejects a port with no host", pkg.ClaimModeShadow, "http://:9090", "self", "must carry a host"},
		{"shadow rejects a non-http scheme", pkg.ClaimModeShadow, "ftp://git-rest", "self", "http or https"},
		{"shadow redacts url userinfo", pkg.ClaimModeShadow, "ftp://u:s3cret@git-rest", "self", "http or https"},
		{"shadow rejects an unparseable url", pkg.ClaimModeShadow, "http://git-rest:bad port", "self", "not a valid URL"},
		{"shadow requires a cluster", pkg.ClaimModeShadow, "http://git-rest", "", "CLUSTER"},
		{"on is refused before the cluster check", pkg.ClaimModeOn, "http://git-rest", "", "not available yet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &application{GitRestURL: tc.url, Cluster: tc.cluster}
			err := a.validateClaimTransport(ctx, tc.mode)
			if tc.want == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.want != "" && err == nil {
				t.Fatal("expected an error")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected the error to contain %q, got %q", tc.want, err.Error())
			}
			// A URL carrying userinfo must never reach the error string.
			if err != nil && strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("error leaked the URL userinfo: %q", err.Error())
			}
		})
	}
}

// TestClaimTransportFieldTags pins the env names the claim transport reads, and
// that the gateway setting carries a Secret NAME rather than a value.
func TestClaimTransportFieldTags(t *testing.T) {
	typ := reflect.TypeOf(application{})
	for _, tc := range []struct{ field, env, arg string }{
		{"GitRestURL", "GITREST_URL", "gitrest-url"},
		{"GitRestGatewaySecret", "GITREST_GATEWAY_SECRET", "gitrest-gateway-secret"},
	} {
		f, ok := typ.FieldByName(tc.field)
		if !ok {
			t.Fatalf("field %s missing", tc.field)
		}
		if got := f.Tag.Get("env"); got != tc.env {
			t.Fatalf("%s env = %q, want %q", tc.field, got, tc.env)
		}
		if got := f.Tag.Get("arg"); got != tc.arg {
			t.Fatalf("%s arg = %q, want %q", tc.field, got, tc.arg)
		}
		if got := f.Tag.Get("required"); got != "false" {
			t.Fatalf("%s must stay optional, got required=%q", tc.field, got)
		}
	}
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
				ReconcileInterval: time.Minute,
			}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reconcile-pass-timeout"))
		})

		It("rejects a non-positive interval before the k8s client", func() {
			a := &application{
				VaultName:            "personal",
				ReconcilePassTimeout: time.Minute,
			}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reconcile-interval"))
		})
	})

	Describe("TaskVault", func() {
		It("AC8: defaults to VAULT_NAME when unset", func() {
			a := &application{VaultName: "openclaw"}
			Expect(a.effectiveTaskVault()).To(Equal("openclaw"))
		})

		It("AC8: uses TASK_VAULT when set and leaves VAULT_NAME untouched", func() {
			a := &application{VaultName: "shared", TaskVault: "personal"}
			Expect(a.effectiveTaskVault()).To(Equal("personal"))
			Expect(a.VaultName).To(Equal("shared"))
		})

		It("AC8: rejects a malformed TASK_VAULT, naming the setting", func() {
			a := &application{
				VaultName:            "personal",
				TaskVault:            "Bad_Vault",
				ReconcilePassTimeout: time.Minute,
				ReconcileInterval:    time.Minute,
			}
			err := a.Run(context.Background(), nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("task-vault"))
		})

		It("AC9: validates with no git-rest settings", func() {
			a := &application{
				VaultName:            "personal",
				ReconcilePassTimeout: time.Minute,
				ReconcileInterval:    time.Minute,
			}
			Expect(a.validate(context.Background())).To(BeNil())
		})
	})
})
