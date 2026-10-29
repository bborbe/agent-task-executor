// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	stderrors "errors"
	"strings"
	"sync"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"
	"github.com/golang/glog"

	"github.com/bborbe/agent-task-executor/pkg/metrics"
)

// ClaimKeySchemeVersion names the claim-key derivation this binary uses. It
// must be bumped in the same change that alters the key strings DeriveClaimKey
// or DeriveClaimKeysWithBody produce.
//
// Two executors that derive different strings for the same task cannot exclude
// each other: each acquires its own ref, each reports success, and the task runs
// twice. That is the failure a leased claim exists to prevent, and it arrives
// through the mechanism's own evolution rather than through a bug. The version
// is what lets a cluster notice it is not deriving what its peers derive.
//
//	version 1 — "claims/repo/<owner>-<repo>" hyphen join, pr-review on the repo key
//	version 2 — "claims/repo/<owner>/<repo>" slash join, pr-review keyed per pull request
//
// The slash join (f3ac37a, v0.41.0) and the per-pull-request key (ae14e78,
// v0.38.2) are both part of version 2. A version names the derivation as a
// whole rather than one edit to it, because only the whole derivation has to
// agree for two clusters to exclude each other.
const ClaimKeySchemeVersion = "v2"

// ClaimSchemeRef is the shared ref that declares which claim-key scheme the
// fleet is running. It lives in git-rest beside the claims, so every cluster
// reads and writes one value and the declaration flips atomically instead of
// rolling cluster by cluster — which is what makes a refusal safe to act on.
//
// It carries no lease semantics: it is a declaration, never held or renewed. It
// sits under "claims/" but outside every prefix the executor lists, so recovery
// and the reaper never see it, and TargetFromKey reports it as unaddressable
// rather than as a claim.
const ClaimSchemeRef = "claims/scheme"

// claimSchemeLease is the lease written with the declaration. Nothing renews the
// ref and nothing reads its lease, so the value only has to outlive the
// declaration: ten years is "never" without an unrepresentable timestamp.
const claimSchemeLease = 10 * 365 * 24 * libtime.Hour

// ErrSchemeMismatch is returned by Check when the fleet's declared claim-key
// scheme differs from the one this binary derives. It is the only refusal the
// guard makes; every other failure admits the claim.
var ErrSchemeMismatch = stderrors.New("claim key scheme mismatch")

// SchemeGuard reports whether this executor may claim at all, by comparing the
// scheme it derives against the scheme the fleet has declared.
type SchemeGuard interface {
	// Check admits the claim when this binary's scheme matches the declared one,
	// or when no declaration exists yet — which it then creates. It refuses,
	// returning an error wrapping ErrSchemeMismatch, only on a positive mismatch.
	//
	// A read failure is admitted, deliberately. An unreachable git-rest must not
	// stop a fleet that claims fine without this guard, so failing closed there
	// would trade one outage class for another. The guard narrows the duplicate
	// window; it never becomes the reason work stops.
	Check(ctx context.Context) error
}

// NewSchemeGuard creates the guard. cluster is written into the declaration and
// is the cluster that won the bootstrap, which is the useful detail when a
// mismatch has to be explained later.
func NewSchemeGuard(
	refClient ClaimRefClient,
	cluster string,
	currentDateTime libtime.CurrentDateTimeGetter,
) SchemeGuard {
	return &schemeGuard{
		refClient:       refClient,
		cluster:         cluster,
		currentDateTime: currentDateTime,
	}
}

type schemeGuard struct {
	refClient       ClaimRefClient
	cluster         string
	currentDateTime libtime.CurrentDateTimeGetter
	// One sync.Once per outcome, not one for the guard. Check runs on every
	// spawn, so an unguarded log line would be one per dispatch pass and the
	// counter carries the rate. A single shared Once would be worse than that:
	// a process that booted while the fleet agreed consumes it on "confirmed",
	// and the mismatch that arrives when the fleet moves is then never logged —
	// exactly the event this guard exists to make visible.
	confirmedOnce    sync.Once
	bootstrappedOnce sync.Once
	mismatchOnce     sync.Once
	unreadableOnce   sync.Once
}

// Check implements SchemeGuard.
func (g *schemeGuard) Check(ctx context.Context) error {
	declared, found, err := g.read(ctx)
	if err != nil {
		return g.admitUnreadable(err)
	}
	if !found {
		return g.bootstrap(ctx)
	}
	if declared != ClaimKeySchemeVersion {
		return g.refuse(ctx, declared)
	}
	g.confirm()
	return nil
}

// bootstrap writes this binary's scheme as the fleet declaration, which is the
// atomic flip point: whichever cluster reaches an empty store first declares for
// everyone, and every other cluster then compares against that value.
//
// A lost compare-and-swap is not an error. It means a peer declared first, so
// the winner's value is read back and judged exactly as a normal check would
// judge it. Two clusters bootstrapping at once therefore converge on one value
// instead of each believing it declared.
func (g *schemeGuard) bootstrap(ctx context.Context) error {
	leaseUntil := g.currentDateTime.Now().Add(claimSchemeLease)
	_, err := g.refClient.Put(
		ctx,
		ClaimSchemeRef,
		"",
		g.cluster,
		ClaimKeySchemeVersion,
		leaseUntil,
	)
	if err == nil {
		metrics.ClaimSchemeTotal.WithLabelValues("bootstrapped").Inc()
		g.bootstrappedOnce.Do(func() {
			glog.Infof(
				"event=claim_scheme_bootstrapped scheme=%s key=%s cluster=%s",
				ClaimKeySchemeVersion, ClaimSchemeRef, g.cluster,
			)
		})
		return nil
	}
	if !stderrors.Is(err, ErrLeaseConflict) {
		return g.admitUnreadable(err)
	}
	declared, found, readErr := g.read(ctx)
	if readErr != nil {
		return g.admitUnreadable(readErr)
	}
	if !found {
		// The declaration vanished between the lost write and this read. Nothing
		// to compare against, so admit; the next spawn bootstraps again.
		return g.admitUnreadable(
			errors.Errorf(ctx, "declaration vanished after a lost create"),
		)
	}
	if declared != ClaimKeySchemeVersion {
		return g.refuse(ctx, declared)
	}
	g.confirm()
	return nil
}

// confirm records a check whose declaration matches this binary's scheme.
func (g *schemeGuard) confirm() {
	metrics.ClaimSchemeTotal.WithLabelValues("confirmed").Inc()
	g.confirmedOnce.Do(func() {
		glog.Infof(
			"event=claim_scheme_confirmed scheme=%s key=%s",
			ClaimKeySchemeVersion, ClaimSchemeRef,
		)
	})
}

// refuse records and returns the one refusal the guard makes.
func (g *schemeGuard) refuse(ctx context.Context, declared string) error {
	metrics.ClaimSchemeTotal.WithLabelValues("mismatch").Inc()
	g.mismatchOnce.Do(func() {
		glog.Errorf(
			"event=claim_scheme_mismatch declared=%s mine=%s key=%s refused=true",
			declared, ClaimKeySchemeVersion, ClaimSchemeRef,
		)
	})
	return errors.Wrapf(
		ctx,
		ErrSchemeMismatch,
		"fleet declares %q, this executor derives %q",
		declared,
		ClaimKeySchemeVersion,
	)
}

// admitUnreadable records a check that could not reach a verdict and admits the
// claim.
func (g *schemeGuard) admitUnreadable(err error) error {
	metrics.ClaimSchemeTotal.WithLabelValues("unreadable").Inc()
	g.unreadableOnce.Do(func() {
		glog.Warningf(
			"event=claim_scheme_unreadable scheme=%s err=%v admitted=true",
			ClaimKeySchemeVersion, err,
		)
	})
	return nil
}

// read returns the declared scheme and whether a declaration exists. The value
// is carried in the ref's task id field: the declaration is a string, and the
// ref API has no other free-text slot that is not already meaningful there. The
// cluster that wrote it stays in the ref's cluster field.
//
// The list is by exact prefix and the match is by exact key, so a stray ref
// under "claims/scheme/…" cannot be mistaken for the declaration.
func (g *schemeGuard) read(ctx context.Context) (string, bool, error) {
	refs, err := g.refClient.List(ctx, ClaimSchemeRef)
	if err != nil {
		return "", false, errors.Wrapf(ctx, err, "list claim scheme declaration")
	}
	for _, ref := range refs {
		if strings.TrimPrefix(ref.Ref, "refs/") != ClaimSchemeRef {
			continue
		}
		return ref.TaskID, true, nil
	}
	return "", false, nil
}
