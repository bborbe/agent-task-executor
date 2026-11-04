// Copyright (c) 2026 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pkg

import (
	"regexp"
	"strconv"
)

// redactionMinSecretLength is the shortest value that is treated as a secret
// when it appears on the right-hand side of a secret-named assignment. A
// shorter value is left readable on purpose: a lookup key is a reference, and
// a log tail whose references are all replaced is worthless for triage.
const redactionMinSecretLength = 13

// redactionPlaceholder is what every recognised credential run is replaced
// with. It is fixed rather than removed so a reader of the captured tail can
// see that something was taken out.
const redactionPlaceholder = "<redacted>"

// The patterns are compiled once at package level: redactLog runs on every
// failure publish, and compiling five regexps per call would be wasted work on
// a path that already does a network read.
var (
	// logRedactPEM matches a whole PEM private-key block, across newlines.
	logRedactPEM = regexp.MustCompile(
		`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`,
	)
	// logRedactGitHub matches a GitHub token: ghp_, gho_, ghu_, ghs_ or ghr_
	// followed by at least twenty alphanumerics.
	logRedactGitHub = regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`)
	// logRedactURL matches URL userinfo — scheme (group 1), user, colon,
	// password, at-sign. The password run stops at the first character that
	// could start the host or the path.
	logRedactURL = regexp.MustCompile(`(://)[^/\s@:]+:[^/\s@]+@`)
	// logRedactBearer matches an Authorization bearer token, keeping the word
	// "Bearer" and its whitespace (group 1).
	logRedactBearer = regexp.MustCompile(`(?i)(Bearer\s+)\S{8,}`)
	// logRedactAssign matches an assignment whose KEY is secret-shaped and
	// whose VALUE is at least redactionMinSecretLength characters. Group 1 is
	// the key plus the separator, so the key survives in the output. The value
	// run is non-whitespace, which covers a quoted logfmt field only up to its
	// first space — a deliberate limit, see redactLog's false-positive note.
	logRedactAssign = regexp.MustCompile(
		`(?i)([A-Za-z0-9_.-]*(?:token|secret|passw|pem|dsn|credential|api[_-]?key|auth)[A-Za-z0-9_.-]*\s*[=:]\s*)\S{` +
			strconv.Itoa(
				redactionMinSecretLength,
			) + `,}`,
	)
)

// redactLog returns text with every recognised credential-shaped run replaced
// by redactionPlaceholder. It is a pure function: no logging, no error return,
// no context, no I/O. Text with nothing to redact is returned byte-for-byte
// unchanged, including its newlines and its trailing newline.
//
// The classes are applied specific-first, the generic assignment last, because
// a specific rule's output must not be re-eaten by the generic one. Ordering
// alone does not guarantee that: a host carrying a keyword is still re-matched
// (`https://<redacted>@auth.example.com:443/oauth2/token`), and that is
// accepted — the credential is gone, which is what matters.
//
// The generic class keys on the NAME, not on length alone. The executor's own
// log lines are logfmt (`event=… task=… job=… reason=…`, see
// pkg/result_publisher.go), so a pure length floor would replace every field
// with the placeholder and ship a tail that cannot be triaged. With the
// key-shape requirement, that exact logfmt line passes through unchanged while
// `ANTHROPIC_AUTH_TOKEN=0123456789abc` becomes `ANTHROPIC_AUTH_TOKEN=<redacted>`.
//
// Residual risk: a credential shape this table does not recognise is written
// as-is. That is a bug, not a tuning change — adding a class means adding an
// entry to the table and a case to the test.
//
// Accepted false positive: a key whose name merely CONTAINS a keyword is
// redacted even when it holds nothing secret (`author=`, `AUTH_URL=`,
// `token_count=`). The alternative is a tail of placeholders, which is the
// worse trade for a record whose whole purpose is diagnosis.
func redactLog(text string) string {
	text = logRedactPEM.ReplaceAllString(text, redactionPlaceholder)
	text = logRedactGitHub.ReplaceAllString(text, redactionPlaceholder)
	text = logRedactURL.ReplaceAllString(text, "${1}"+redactionPlaceholder+"@")
	text = logRedactBearer.ReplaceAllString(text, "${1}"+redactionPlaceholder)
	text = logRedactAssign.ReplaceAllString(text, "${1}"+redactionPlaceholder)
	return text
}
