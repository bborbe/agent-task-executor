// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/bborbe/agent-task-executor/pkg/handler"
)

var _ = Describe("VaultMatcher", func() {
	Describe("NewVaultMatcher", func() {
		It("accepts the executor's own vault", func() {
			m := handler.NewVaultMatcher("openclaw", nil)
			Expect(m.Accepts("openclaw")).To(BeTrue())
			Expect(m.Vault()).To(Equal("openclaw"))
		})

		It("rejects a foreign vault when no alias is configured", func() {
			m := handler.NewVaultMatcher("openclaw", nil)
			Expect(m.Accepts("private-agent")).To(BeFalse())
		})

		It("never owns a task carrying no stamped vault", func() {
			m := handler.NewVaultMatcher("openclaw", []string{"private-agent"})
			Expect(m.Accepts("")).To(BeFalse())
		})

		It("accepts every non-empty alias it was given", func() {
			m := handler.NewVaultMatcher("openclaw", []string{"private-agent", ""})
			Expect(m.Accepts("private-agent")).To(BeTrue())
			Expect(m.Accepts("trading")).To(BeFalse())
		})
	})

	Describe("rename tolerance", func() {
		It("admits a private-agent stamp on an openclaw executor (SC3a)", func() {
			m := handler.NewVaultMatcher("openclaw", handler.DefaultVaultAliases("openclaw"))
			Expect(m.Accepts("private-agent")).To(BeTrue())
		})

		It("admits an openclaw stamp on a private-agent executor (SC3b)", func() {
			m := handler.NewVaultMatcher(
				"private-agent",
				handler.DefaultVaultAliases("private-agent"),
			)
			Expect(m.Accepts("openclaw")).To(BeTrue())
		})
	})

	Describe("the alias set is not a wildcard", func() {
		It("still drops a trading stamp on an openclaw executor (SC3c)", func() {
			m := handler.NewVaultMatcher("openclaw", handler.DefaultVaultAliases("openclaw"))
			Expect(m.Accepts("trading")).To(BeFalse())
		})

		It("still drops a private-agent stamp on a trading executor (SC3d)", func() {
			m := handler.NewVaultMatcher("trading", handler.DefaultVaultAliases("trading"))
			Expect(m.Accepts("private-agent")).To(BeFalse())
		})
	})

	Describe("DefaultVaultAliases", func() {
		It("returns nil for a vault that is not mid-rename", func() {
			Expect(handler.DefaultVaultAliases("trading")).To(BeNil())
			Expect(handler.DefaultVaultAliases("personal")).To(BeNil())
		})

		It("returns the same set for both ends of the pair", func() {
			Expect(handler.DefaultVaultAliases("openclaw")).To(
				ConsistOf("openclaw", "private-agent"),
			)
			Expect(handler.DefaultVaultAliases("private-agent")).To(
				ConsistOf("openclaw", "private-agent"),
			)
		})
	})
})
