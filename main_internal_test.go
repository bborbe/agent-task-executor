// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
