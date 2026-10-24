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

		It("still drops a trading stamp on a personal executor", func() {
			m := handler.NewVaultMatcher("personal", handler.DefaultVaultAliases("personal"))
			Expect(m.Accepts("trading")).To(BeFalse())
			Expect(m.Accepts("private-trading")).To(BeFalse())
		})

		It("still drops a personal stamp on a trading executor", func() {
			m := handler.NewVaultMatcher("trading", handler.DefaultVaultAliases("trading"))
			Expect(m.Accepts("personal")).To(BeFalse())
			Expect(m.Accepts("private-personal")).To(BeFalse())
		})
	})

	Describe("DefaultVaultAliases", func() {
		It("returns nil for a vault that is not mid-rename", func() {
			Expect(handler.DefaultVaultAliases("shared")).To(BeNil())
			Expect(handler.DefaultVaultAliases("")).To(BeNil())
		})

		// Every pair the fleet stamps both ways. An entry present in one direction
		// only re-creates the drop on the other side, which is the whole bug.
		pairs := [][2]string{
			{"openclaw", "private-agent"},
			{"personal", "private-personal"},
			{"trading", "private-trading"},
		}
		for _, pair := range pairs {
			bare, canonical := pair[0], pair[1]
			It("returns the same set for both ends of the "+bare+" pair", func() {
				Expect(handler.DefaultVaultAliases(bare)).To(ConsistOf(bare, canonical))
				Expect(handler.DefaultVaultAliases(canonical)).To(ConsistOf(bare, canonical))
			})

			It("admits "+canonical+" on the "+bare+" executor and back", func() {
				forward := handler.NewVaultMatcher(bare, handler.DefaultVaultAliases(bare))
				Expect(forward.Accepts(canonical)).To(BeTrue())
				backward := handler.NewVaultMatcher(
					canonical,
					handler.DefaultVaultAliases(canonical),
				)
				Expect(backward.Accepts(bare)).To(BeTrue())
			})
		}
	})
})
