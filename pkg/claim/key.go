// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package claim

import (
	"context"
	"strconv"
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

// DeriveClaimKeyWithBody maps a task to the claim key it uses. It behaves like
// DeriveClaimKey, except that a pr-review task whose body carries the task
// repository's own pull-request URL is keyed per pull request. The second
// return value reports that a pr-review task fell back to the repository key;
// it is meaningful only when the error is nil.
func DeriveClaimKeyWithBody(
	ctx context.Context,
	taskType string,
	taskID string,
	owner string,
	repo string,
	body string,
) (string, bool, error) {
	if taskType != "pr-review" {
		key, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)
		return key, false, err
	}
	owner = strings.ToLower(owner)
	repo = strings.ToLower(repo)
	number, found := pullRequestNumberFromBody(owner, repo, body)
	if found {
		if err := validateClaimComponent(ctx, "owner", owner); err != nil {
			return "", false, err
		}
		if err := validateClaimComponent(ctx, "repo", repo); err != nil {
			return "", false, err
		}
		if err := validateClaimComponent(ctx, "pull request number", number); err != nil {
			return "", false, err
		}
		return "claims/pr/" + owner + "-" + repo + "-" + number, false, nil
	}
	key, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)
	return key, true, err
}

// pullRequestNumberFromBody returns the first standalone pull-request URL in
// body that names owner/repo, with the number in canonical decimal form.
func pullRequestNumberFromBody(owner, repo, body string) (string, bool) {
	if owner == "" || repo == "" {
		return "", false
	}
	for _, line := range strings.Split(body, "\n") {
		if number, ok := pullRequestNumberFromLine(owner, repo, strings.TrimSpace(line)); ok {
			return number, true
		}
	}
	return "", false
}

// pullRequestNumberFromLine returns the number in line when line is exactly
// https://github.com/<owner>/<repo>/pull/<n>, and false otherwise.
func pullRequestNumberFromLine(owner, repo, line string) (string, bool) {
	const prefix = "https://github.com/"
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	parts := strings.Split(strings.TrimPrefix(line, prefix), "/")
	if len(parts) != 4 {
		return "", false
	}
	if parts[2] != "pull" {
		return "", false
	}
	if !strings.EqualFold(parts[0], owner) || !strings.EqualFold(parts[1], repo) {
		return "", false
	}
	raw := parts[3]
	if len(raw) < 1 || len(raw) > 10 {
		return "", false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return "", false
		}
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return "", false
	}
	return strconv.Itoa(value), true
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
