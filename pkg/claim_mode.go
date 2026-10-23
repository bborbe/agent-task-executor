// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"context"
	stderrors "errors"

	"github.com/bborbe/errors"
)

// ClaimMode is the closed set of dispatch claim modes.
type ClaimMode string

const (
	// ClaimModeOff constructs no claim client and preserves pre-claim dispatch.
	ClaimModeOff ClaimMode = "off"
	// ClaimModeShadow takes claims but never blocks a dispatch on one.
	ClaimModeShadow ClaimMode = "shadow"
	// ClaimModeOn makes a held claim a precondition of the Job.
	ClaimModeOn ClaimMode = "on"
)

// ErrClaimModeInvalid is returned by ParseClaimMode for an unrecognised or
// explicitly empty value.
var ErrClaimModeInvalid = stderrors.New("claim mode invalid")

// TakesClaim reports whether this mode takes a claim, and so needs the claim
// transport: a ref client, its git-rest URL and its gateway secret.
//
// It is an allow-list, not a check for off. A zero-value or otherwise unparsed
// mode reports false, so an unrecognised mode constructs no transport and reads
// no secret instead of failing open.
func (m ClaimMode) TakesClaim() bool {
	switch m {
	case ClaimModeShadow, ClaimModeOn:
		return true
	}
	return false
}

// ParseClaimMode parses the CLAIM_MODE value. present reports whether the
// variable exists in the environment, which distinguishes an unset variable
// (off) from an explicitly empty one (invalid).
func ParseClaimMode(ctx context.Context, value string, present bool) (ClaimMode, error) {
	if !present {
		return ClaimModeOff, nil
	}
	switch ClaimMode(value) {
	case ClaimModeOff, ClaimModeShadow, ClaimModeOn:
		return ClaimMode(value), nil
	}
	return "", errors.Wrapf(
		ctx,
		ErrClaimModeInvalid,
		"claim mode %q invalid, accepted: off|shadow|on",
		value,
	)
}
