// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"reflect"

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
})
