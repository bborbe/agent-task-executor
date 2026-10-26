// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package claimtest provides test doubles for the claim package's typed client.
//
// It exists so a test that already owns a compare-and-swap ref store can drive
// the lease manager without standing up an HTTP server: the adapter applies the
// same rules vault-api applies — a live claim refuses an acquire, an expired one
// is taken over, a stale token is a lost lease — over whatever ClaimRefClient
// the test supplies. The HTTP-level contract itself is covered separately by
// vault_api_client_test.go.
package claimtest

import (
	"context"
	"strings"

	"github.com/bborbe/errors"
	libtime "github.com/bborbe/time"

	"github.com/bborbe/agent-task-executor/pkg/claim"
)

// Client adapts a ClaimRefClient store to the typed ClaimClient.
type Client struct {
	refs claim.ClaimRefClient
	now  func() libtime.DateTime
}

// NewClient returns a ClaimClient over refs. now supplies the clock a lease is
// computed against, so a test can move time without touching the store.
func NewClient(refs claim.ClaimRefClient, now func() libtime.DateTime) claim.ClaimClient {
	return &Client{refs: refs, now: now}
}

// Acquire takes the claim for holder, taking over an expired lease.
func (c *Client) Acquire(
	ctx context.Context,
	target claim.ClaimTarget,
	holder string,
	claimantTaskID string,
	ttlSeconds int,
) (*claim.Claim, error) {
	key, ok := claim.KeyFor(target)
	if !ok {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "acquire %s", target.String())
	}
	if problem := claimantError(target, claimantTaskID); problem != "" {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "acquire %s: %s",
			target.String(), problem)
	}
	expected, held, err := c.liveClaim(ctx, key)
	if err != nil {
		return nil, err
	}
	if held != nil {
		return nil, held
	}
	leaseUntil := c.now().Add(libtime.Second * libtime.Duration(ttlSeconds))
	sha, err := c.refs.Put(
		ctx,
		key,
		expected,
		holder,
		transportTaskID(target, claimantTaskID),
		leaseUntil,
	)
	if err != nil {
		if errors.Is(err, claim.ErrLeaseConflict) {
			return nil, &claim.ClaimHeldError{Holder: "", LeaseUntil: c.now()}
		}
		return nil, err
	}
	return newClaim(target, holder, claimantTaskID, leaseUntil, sha), nil
}

// Renew extends the lease the token names. A token that is not current is a lost
// lease, never a silent success.
func (c *Client) Renew(
	ctx context.Context,
	target claim.ClaimTarget,
	holder string,
	leaseToken string,
	claimantTaskID string,
	ttlSeconds int,
) (*claim.Claim, error) {
	key, ok := claim.KeyFor(target)
	if !ok {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "renew %s", target.String())
	}
	if holder == "" {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "renew %s: empty holder",
			target.String())
	}
	if problem := claimantError(target, claimantTaskID); problem != "" {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "renew %s: %s",
			target.String(), problem)
	}
	leaseUntil := c.now().Add(libtime.Second * libtime.Duration(ttlSeconds))
	sha, err := c.refs.Put(
		ctx,
		key,
		leaseToken,
		holder,
		transportTaskID(target, claimantTaskID),
		leaseUntil,
	)
	if err != nil {
		if errors.Is(err, claim.ErrLeaseConflict) {
			return nil, errors.Wrapf(ctx, claim.ErrLeaseLost, "renew %s", target.String())
		}
		return nil, err
	}
	return newClaim(target, holder, claimantTaskID, leaseUntil, sha), nil
}

// Release drops the claim the token names.
func (c *Client) Release(
	ctx context.Context,
	target claim.ClaimTarget,
	leaseToken string,
) error {
	key, ok := claim.KeyFor(target)
	if !ok {
		return errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "release %s", target.String())
	}
	err := c.refs.Delete(ctx, key, leaseToken)
	if err != nil {
		if errors.Is(err, claim.ErrLeaseConflict) {
			return errors.Wrapf(ctx, claim.ErrLeaseLost, "release %s", target.String())
		}
		return err
	}
	return nil
}

// Read returns the current claim, or ErrClaimFree when there is none.
func (c *Client) Read(ctx context.Context, target claim.ClaimTarget) (*claim.Claim, error) {
	key, ok := claim.KeyFor(target)
	if !ok {
		return nil, errors.Wrapf(ctx, claim.ErrClaimTargetInvalid, "read %s", target.String())
	}
	ref, err := c.exact(ctx, key)
	if err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, errors.Wrapf(ctx, claim.ErrClaimFree, "read %s", target.String())
	}
	return claimFromRef(*ref, target)
}

// List returns every claim, optionally narrowed to one holder.
func (c *Client) List(ctx context.Context, holder string) ([]claim.Claim, error) {
	refs, err := c.refs.List(ctx, "claims/")
	if err != nil {
		return nil, err
	}
	result := make([]claim.Claim, 0, len(refs))
	for _, ref := range refs {
		target, ok := claim.TargetFromKey(strings.TrimPrefix(ref.Ref, "refs/"))
		if !ok {
			continue
		}
		if holder != "" && ref.Cluster != holder {
			continue
		}
		parsed, err := claimFromRef(ref, target)
		if err != nil {
			return nil, err
		}
		result = append(result, *parsed)
	}
	return result, nil
}

// liveClaim returns the compare-and-swap token an acquire must carry, and the
// claim that blocks it when its lease has not passed.
func (c *Client) liveClaim(
	ctx context.Context,
	key string,
) (string, *claim.ClaimHeldError, error) {
	ref, err := c.exact(ctx, key)
	if err != nil {
		return "", nil, err
	}
	if ref == nil {
		return "", nil, nil
	}
	until, parseErr := libtime.ParseDateTime(ctx, ref.LeaseUntil)
	if parseErr != nil || !until.After(c.now()) {
		// Unparsable reads as expired, exactly as the server treats it: a claim
		// whose lease nobody can read must not block the key forever.
		return ref.Sha, nil, nil
	}
	return "", &claim.ClaimHeldError{Holder: ref.Cluster, LeaseUntil: *until}, nil
}

// exact returns the ref whose full name is refs/<key>.
func (c *Client) exact(ctx context.Context, key string) (*claim.ClaimRef, error) {
	refs, err := c.refs.List(ctx, key)
	if err != nil {
		return nil, err
	}
	want := "refs/" + key
	for _, ref := range refs {
		if ref.Ref == want {
			found := ref
			return &found, nil
		}
	}
	return nil, nil
}

// claimantError mirrors the server's claimant-task-id contract, so a test that
// violates it fails the way production would.
func claimantError(target claim.ClaimTarget, claimantTaskID string) string {
	if target.TaskID != "" && claimantTaskID != "" {
		return "a task claim must not carry a claimant task id"
	}
	if target.TaskID == "" && claimantTaskID == "" {
		return "a repository or pull-request claim requires a claimant task id"
	}
	return ""
}

// transportTaskID is the task id stored on the ref: the addressed task for a
// task claim, the claiming run's own id otherwise.
func transportTaskID(target claim.ClaimTarget, claimantTaskID string) string {
	if target.TaskID != "" {
		return target.TaskID
	}
	return claimantTaskID
}

func newClaim(
	target claim.ClaimTarget,
	holder string,
	claimantTaskID string,
	leaseUntil libtime.DateTime,
	token string,
) *claim.Claim {
	return &claim.Claim{
		Scope:      scopeOf(target),
		ID:         idOf(target),
		Holder:     holder,
		TaskID:     transportTaskID(target, claimantTaskID),
		LeaseUntil: leaseUntil,
		LeaseToken: token,
	}
}

func claimFromRef(ref claim.ClaimRef, target claim.ClaimTarget) (*claim.Claim, error) {
	result := &claim.Claim{
		Scope:      scopeOf(target),
		ID:         idOf(target),
		Holder:     ref.Cluster,
		TaskID:     ref.TaskID,
		LeaseToken: ref.Sha,
	}
	until, err := libtime.ParseDateTime(context.Background(), ref.LeaseUntil)
	if err != nil {
		result.LeaseUnreadable = true
		return result, nil
	}
	result.LeaseUntil = *until
	return result, nil
}

func scopeOf(target claim.ClaimTarget) string {
	switch {
	case target.TaskID != "":
		return "task"
	case target.Number != "":
		return "pr"
	default:
		return "repo"
	}
}

func idOf(target claim.ClaimTarget) string {
	switch {
	case target.TaskID != "":
		return target.TaskID
	case target.Number != "":
		return target.Owner + "/" + target.Repo + "/" + target.Number
	default:
		return target.Owner + "/" + target.Repo
	}
}
