// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

// VaultMatcher decides whether a task's stamped vault belongs to this executor.
//
// A task is owned when its stamp equals the executor's own task vault, or is one
// of that vault's accepted aliases. The alias set exists because one Obsidian
// vault is stamped under more than one slug while a rename is in flight across
// clusters: plain equality makes every cluster that disagrees silently drop the
// other's tasks, and that drop is logged rather than raised, so it reads as a
// quiet fleet.
//
// The set belongs to the matcher, never to a shared global. A single list
// applied to every executor would make each one accept the others' vaults —
// the same bug in the opposite direction, and the reason the tests assert both
// the admit and the drop.
type VaultMatcher struct {
	vault    string
	accepted map[string]struct{}
}

// NewVaultMatcher returns a matcher for vault that accepts vault itself plus
// every name in aliases. An empty vault accepts nothing.
func NewVaultMatcher(vault string, aliases []string) VaultMatcher {
	accepted := make(map[string]struct{}, len(aliases)+1)
	if vault != "" {
		accepted[vault] = struct{}{}
	}
	for _, alias := range aliases {
		if alias != "" {
			accepted[alias] = struct{}{}
		}
	}
	return VaultMatcher{vault: vault, accepted: accepted}
}

// Accepts reports whether a task stamped with stamp is owned by this executor.
// An empty stamp is never owned: a task carrying no stamped vault belongs to no
// executor.
func (m VaultMatcher) Accepts(stamp string) bool {
	if stamp == "" {
		return false
	}
	_, ok := m.accepted[stamp]
	return ok
}

// Vault returns the executor's own task vault, for logging.
func (m VaultMatcher) Vault() string {
	return m.vault
}

// defaultVaultAliases maps an executor's task vault to every slug the same
// Obsidian vault is stamped under. It is the fallback when VAULT_ALIASES is
// unset, so no deployment has to carry the value.
//
// Keep each entry symmetric: an executor carrying either name must accept the
// other, or the cluster still stamping the old slug keeps dropping.
var defaultVaultAliases = map[string][]string{
	"openclaw":         {"openclaw", "private-agent"},
	"private-agent":    {"openclaw", "private-agent"},
	"personal":         {"personal", "private-personal"},
	"private-personal": {"personal", "private-personal"},
	"trading":          {"trading", "private-trading"},
	"private-trading":  {"trading", "private-trading"},
}

// DefaultVaultAliases returns the built-in alias set for vault, or nil when the
// vault has no known alias. A nil result means exact equality, which is the
// behaviour of every vault that is not mid-rename.
func DefaultVaultAliases(vault string) []string {
	return defaultVaultAliases[vault]
}
