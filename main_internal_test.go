// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
