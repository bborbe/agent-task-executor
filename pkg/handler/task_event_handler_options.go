// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package handler

import (
	pkg "github.com/bborbe/agent-task-executor/pkg"
	"github.com/bborbe/agent-task-executor/pkg/claim"
	"github.com/bborbe/agent-task-executor/pkg/spawner"
)

// HandlerOption customises a taskEventHandler at construction. It replaces the
// former single-purpose variadic so a new optional dependency does not widen the
// positional parameter list every call site has to repeat.
type HandlerOption func(*taskEventHandler)

// WithClaimJobControl attaches the job control the claim lease manager acts on.
func WithClaimJobControl(control claim.JobControl) HandlerOption {
	return func(h *taskEventHandler) {
		h.claimJobControl = control
	}
}

// WithClaimClientFactory supplies the typed vault-api claim client. A
// claim-taking mode without one fails at lease-manager construction rather than
// holding nothing, which is why it is not a positional argument: only the modes
// that take a claim need it, and every other caller would pass a nil it never
// uses.
func WithClaimClientFactory(factory pkg.ClaimClientFactory) HandlerOption {
	return func(h *taskEventHandler) {
		h.claimClientFactory = factory
	}
}

// WithSuccessStore supplies the shared success record directly, bypassing the
// claim-ref-client factory the handler would otherwise build it from. It exists
// for tests that need a fake — the store's error path has no other way in, since
// the factory is a boot-time constructor with no failure mode to provoke.
func WithSuccessStore(store claim.SuccessStore) HandlerOption {
	return func(h *taskEventHandler) {
		h.successRecordStore = store
	}
}

// WithVaultAliases replaces the accepted vault names for this executor. It is how
// VAULT_ALIASES reaches the matcher without every call site carrying the value,
// and it is per handler by construction: two executors built in one process each
// get their own set, so neither can accept the other's vault.
func WithVaultAliases(aliases []string) HandlerOption {
	return func(h *taskEventHandler) {
		h.vaultMatcher = NewVaultMatcher(h.vaultMatcher.Vault(), aliases)
	}
}

// WithVaultAPIIndexClient supplies the vault-api task-index reader used by
// the reconcile pass's view/vault divergence check. A nil client (the
// default) skips the check, which is the no-URL deployment's shape.
func WithVaultAPIIndexClient(client claim.VaultAPIIndexClient) HandlerOption {
	return func(h *taskEventHandler) {
		h.vaultIndexClient = client
	}
}

// WithPodQuotaLimiter attaches the reader that bounds admission by the pod quota
// of the agent's priority class. An absent limiter means no quota bound.
func WithPodQuotaLimiter(limiter spawner.PodQuotaLimiter) HandlerOption {
	return func(h *taskEventHandler) {
		h.podQuotaLimiter = limiter
	}
}
