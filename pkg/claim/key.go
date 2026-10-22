// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"strings"

	"github.com/bborbe/errors"
)

// repoKeyedTaskTypes are the task types that share one claim key per
// repository instead of one key per task id.
var repoKeyedTaskTypes = map[string]bool{
	"update-go": true,
	"release":   true,
	"pr-review": true,
	"build-fix": true,
}

// DeriveClaimKey maps a task to the claim key it uses. The four repo-keyed task
// types share one key per repository; every other type is keyed by task id.
// Every component is validated before it is used: empty, "/", "..", a leading
// ".", a trailing ".lock", or a character outside [A-Za-z0-9._-] is rejected.
func DeriveClaimKey(
	ctx context.Context,
	taskType string,
	taskID string,
	owner string,
	repo string,
) (string, error) {
	if repoKeyedTaskTypes[taskType] {
		owner = strings.ToLower(owner)
		repo = strings.ToLower(repo)
		if err := validateClaimComponent(ctx, "owner", owner); err != nil {
			return "", err
		}
		if err := validateClaimComponent(ctx, "repo", repo); err != nil {
			return "", err
		}
		return "claims/repo/" + owner + "-" + repo, nil
	}
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return "", err
	}
	return "claims/task/" + taskID, nil
}

// validateClaimComponent rejects a value that must not appear in a claim key
// path. It is the single validation used for owner, repo and task id.
func validateClaimComponent(ctx context.Context, name, value string) error {
	if value == "" {
		return errors.Errorf(ctx, "claim %s must not be empty", name)
	}
	if strings.Contains(value, "/") {
		return errors.Errorf(ctx, "claim %s must not contain %q", name, "/")
	}
	if strings.Contains(value, "..") {
		return errors.Errorf(ctx, "claim %s must not contain %q", name, "..")
	}
	if strings.HasPrefix(value, ".") {
		return errors.Errorf(ctx, "claim %s must not start with %q", name, ".")
	}
	if strings.HasSuffix(value, ".lock") {
		return errors.Errorf(ctx, "claim %s must not end with %q", name, ".lock")
	}
	for _, r := range value {
		if !isSafeClaimRune(r) {
			return errors.Errorf(ctx, "claim %s contains invalid character %q", name, r)
		}
	}
	return nil
}

// isSafeClaimRune reports whether r is allowed in a claim key component.
func isSafeClaimRune(r rune) bool {
	switch {
	case r >= 'A' && r <= 'Z':
		return true
	case r >= 'a' && r <= 'z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '.' || r == '_' || r == '-':
		return true
	default:
		return false
	}
}
