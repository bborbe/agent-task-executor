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
// repository instead of one key per task id. A pr-review is deliberately absent:
// it is keyed per pull request when its body names one, and per task id when it
// does not. Sharing the per-repository key made every review of one repository
// refuse its siblings at takeover.
var repoKeyedTaskTypes = map[string]bool{
	"update-go": true,
	"release":   true,
	"build-fix": true,
}

// DeriveClaimKey maps a task to the claim key it uses. The three repo-keyed task
// types share one key per repository; every other type is keyed by task id.
// Every component is validated before it is used: empty, "/", "..", a leading
// ".", a trailing ".lock", or a character outside [A-Za-z0-9._-] is rejected.
//
// ⚠️ Owner and repo are joined with "/", not "-", and that is load-bearing.
// A hyphen is legal inside both components, so the old hyphen join was lossy:
// owner "a-b" with repo "c" and owner "a" with repo "b-c" both derived
// "claims/repo/a-b-c" — two distinct repositories aliasing one lock, so the
// mutual exclusion silently did nothing. Because "/" is rejected inside a
// component, the slash join cannot collide. This also matches the streak keys
// (claims/redrive/<taskid>/<phase>/<n>), which were already slash-separated.
//
// ⚠️ The format is shared with vault-api, which reproduces it rather than
// deriving its own. The two must change together: a period where one side
// writes "claims/repo/a/b" and the other reads "claims/repo/a-b" is a period
// where the lock excludes nothing. In-flight claims carry a 5-minute lease, so
// a rolling deploy orphans each one for at most one lease and the next acquire
// takes it over (see AcquireClaimHandler in vault-api, which carries an expired
// ref's own SHA as the compare-and-swap).
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
		return "claims/repo/" + owner + "/" + repo, nil
	}
	if err := validateClaimComponent(ctx, "task id", taskID); err != nil {
		return "", err
	}
	return "claims/task/" + taskID, nil
}

// DeriveClaimKeysWithBody maps a task to the claim refs it uses. The first
// return value is the primary key. The second is the secondary key, which is
// always empty: no task type writes a pair any more, so every claim is a
// single ref. The third reports that a pr-review task carried no usable
// pull-request URL and was keyed by its task id instead; it is meaningful only
// when the error is nil.
//
// A pr-review task whose body carries a standalone pull-request URL is keyed
// per pull request, never by the shared repository key. Making the repository
// key the primary serialised every review of one repository behind a single
// claim: the first holder refused every sibling at takeover with a bare
// lease conflict for as long as it renewed, so no review of that repository
// could start.
func DeriveClaimKeysWithBody(
	ctx context.Context,
	taskType string,
	taskID string,
	owner string,
	repo string,
	body string,
) (string, string, bool, error) {
	if taskType != "pr-review" {
		primary, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)
		return primary, "", false, err
	}
	owner = strings.ToLower(owner)
	repo = strings.ToLower(repo)
	number, found := pullRequestNumberFromBody(owner, repo, body)
	if found {
		if err := validateClaimComponent(ctx, "owner", owner); err != nil {
			return "", "", false, errors.Wrapf(ctx, err, "derive pr claim key")
		}
		if err := validateClaimComponent(ctx, "repo", repo); err != nil {
			return "", "", false, errors.Wrapf(ctx, err, "derive pr claim key")
		}
		if err := validateClaimComponent(ctx, "pull request number", number); err != nil {
			return "", "", false, errors.Wrapf(ctx, err, "derive pr claim key")
		}
		return "claims/pr/" + owner + "/" + repo + "/" + number,
			"",
			false,
			nil
	}
	primary, err := DeriveClaimKey(ctx, taskType, taskID, owner, repo)
	return primary, "", true, err
}

// TargetFromKey parses a claim key back into the typed target it addresses. It
// is the inverse of DeriveClaimKey, and mirrors vault-api's claimScopeFromKey
// exactly — a shape this side accepts and the server does not is a claim the
// executor renews into a 404, and the reverse is a claim it can never re-adopt
// after a restart.
//
// It exists because a held claim is addressed two ways: by the target a caller
// derives when acquiring, and by the key a listing returns on recovery. Without
// the inverse, recovery would have to re-derive a target from a task's body,
// which is the caller's job and not something a listed key can supply.
//
// A key that is not one of the three claim shapes reports false. The redrive
// streak keys (claims/redrive/<taskid>/<phase>/<n>) land here: they share the
// prefix but are counters, never acquired or released, so answering them as
// claims would invite a caller to renew something that is not a lock.
func TargetFromKey(key string) (ClaimTarget, bool) {
	switch {
	case strings.HasPrefix(key, "claims/task/"):
		id := strings.TrimPrefix(key, "claims/task/")
		if id == "" {
			return ClaimTarget{}, false
		}
		return NewTaskClaim(id), true
	case strings.HasPrefix(key, "claims/repo/"):
		parts := strings.Split(strings.TrimPrefix(key, "claims/repo/"), "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return ClaimTarget{}, false
		}
		return NewRepoClaim(parts[0], parts[1]), true
	case strings.HasPrefix(key, "claims/pr/"):
		parts := strings.Split(strings.TrimPrefix(key, "claims/pr/"), "/")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return ClaimTarget{}, false
		}
		return NewPRClaim(parts[0], parts[1], parts[2]), true
	}
	return ClaimTarget{}, false
}

// KeyFor returns the claim key a target addresses, or false when the target is
// not addressable. It is the forward direction of TargetFromKey and produces
// exactly the shapes the server parses back.
func KeyFor(target ClaimTarget) (string, bool) {
	if target.isAmbiguous() {
		return "", false
	}
	if target.TaskID != "" {
		return "claims/task/" + target.TaskID, true
	}
	if target.isPR() &&
		(target.Owner == "" || target.Repo == "" || !validPRNumber(target.Number)) {
		return "", false
	}
	if target.Owner != "" && target.Repo != "" {
		if target.isPR() {
			return "claims/pr/" + target.Owner + "/" + target.Repo + "/" + target.Number, true
		}
		return "claims/repo/" + target.Owner + "/" + target.Repo, true
	}
	return "", false
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
