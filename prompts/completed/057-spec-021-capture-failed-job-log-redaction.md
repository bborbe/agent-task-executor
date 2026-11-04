---
status: approved
spec: [021-capture-failed-job-log-in-failure-section]
created: "2026-11-04T01:20:02Z"
queued: "2026-11-04T01:44:19Z"
---

# Redact credential shapes from captured pod log text

<summary>
- A reusable text scrubber turns credential-shaped strings into a visible placeholder before any log text can be written into a task record
- A GitHub token, a PEM private-key block, a bearer token and a URL carrying userinfo are each removed
- An assignment whose key is secret-named and whose value is 13 or more characters is removed as well, because agent pods carry secrets under names such as `ANTHROPIC_AUTH_TOKEN`
- A short value survives untouched, so a lookup key such as a short reference id stays readable in the failure record
- A value under an ordinary logfmt key (`event=`, `task=`, `job=`) is never redacted, so the captured tail stays diagnosable
- A value at 13 characters is redacted and a 12-character value is kept, so the boundary is a deliberate, documented line rather than an accident of the pattern
- The scrubber replaces a match with a fixed placeholder, so a reader can see that something was removed
- No failure record changes in this change: existing behaviour is untouched
- Every credential class in the shipped set has a case that fails when the class stops working, so a gap shows up as a red check rather than a leaked secret
</summary>

<objective>
Add a pure text scrubber to the `pkg` package that removes credential-shaped material from arbitrary container output, so the next change can capture a failed Job's log tail without writing a secret into a vault file that is committed and pushed unattended.
</objective>

<context>
Read these files before changing anything:

- `pkg/result_publisher.go` — the `## Failure` section is composed here in `publishFailure` (`fmt.Sprintf("## Failure\n\n- **Timestamp:** ...")`). Read it to understand where the scrubbed text will later land. Do **not** modify this file in this prompt.
- `pkg/suite_test.go` — `TestPkg` is the Ginkgo suite entry point for the `pkg` package. It lives in `package pkg_test`.
- `pkg/job_watcher_internal_test.go` — the convention for an internal Ginkgo spec file (`package pkg`, `var _ = Describe(...)`). Internal test files in `package pkg` and external ones in `package pkg_test` are compiled into one test binary, so a spec registered here runs under `TestPkg`. This is how an internal spec reaches an unexported symbol.
- `pkg/zombie_reason.go` — the package's existing style for a closed vocabulary of string classes and its parser.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions, `DescribeTable` / `Entry`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-doc-best-practices.md` — doc-comment shape for an unexported helper that carries a security contract
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — package file structure and the interface → constructor → struct → method pattern. It does **not** cover compiled-regexp tables, so the regexp layout is your own choice.

Verified facts (do not re-derive):
- The repo is Go 1.27, standard library only for this change: `regexp`, `strconv` and `strings`. No new module dependency.
- `make precommit` runs `ensure` (`go mod tidy`), `format` (goimports-reviser, golines at max line length 100), `generate`, `test`, `check`, `addlicense`. A new file needs the BSD license header, which `addlicense` adds automatically.
- Ginkgo specs in `package pkg` and `package pkg_test` share one binary; `-run TestPkg` selects the suite, `-ginkgo.focus` selects specs.
- The executor's own log lines are logfmt: `event=… task=… job=… reason=…` (`pkg/result_publisher.go:549`). This is why the generic class below keys on the **name**, not on length alone — see requirement 3.
</context>

<requirements>
1. **Create `pkg/log_redactor.go`** (`package pkg`) containing the floor constant, the compiled patterns, and the scrubber. Include the BSD license header at the top of the file, matching every other file in the package.

2. **Declare the floor constant exactly as named by the spec:**

   ```go
   // redactionMinSecretLength is the shortest value that is treated as a secret
   // when it appears on the right-hand side of a secret-named assignment. A
   // shorter value is left readable on purpose: a lookup key is a reference, and
   // a log tail whose references are all replaced is worthless for triage.
   const redactionMinSecretLength = 13
   ```

   The generic assignment pattern below MUST be built from this constant (via `strconv.Itoa(redactionMinSecretLength)`), so the implementation and the boundary test cannot drift apart. A package-level `var` initialized by a function call is correct here; a `const` cannot be initialized that way.

3. **Implement `func redactLog(text string) string`.** It returns the input with every recognised credential-shaped run replaced by the literal placeholder `<redacted>`. It is a pure function: no logging, no error return, no `context`, no I/O. An input with nothing to redact is returned byte-for-byte unchanged, including its newlines and its trailing newline.

   Apply the classes in this order — specific shapes first, the generic assignment last — each as a separate `ReplaceAllString` pass over the previous pass's output, replacing with a string that re-emits capture group 1 (`${1}<redacted>`) wherever the pattern has a group — classes 3, 4 and 5 do; classes 1 and 2 have no capture group and use the bare `<redacted>`. A bare replacement on a grouped pattern deletes the prefix the table says to keep, and for class 5 it deletes the key that requirement 4a asserts survives. Order alone is not enough: the generic rule runs on text a specific rule has already rewritten, so it must not re-match what that rule left behind. Requiring a **secret-named key** prevents the common collision — `https` is not secret-named, so an ordinary URL's output is not re-eaten by class 5 — but a host that itself carries a keyword still is (`https://<redacted>@auth.example.com:443/oauth2/token`), so do not rely on the ordering for host fidelity:

   | Order | Class | Recognised shape | Replacement |
   |---|---|---|---|
   | 1 | PEM private-key block | `-----BEGIN <WORDS>PRIVATE KEY-----` through the matching `-----END <WORDS>PRIVATE KEY-----`, inclusive, across newlines | the whole block → `<redacted>` |
   | 2 | GitHub token | `ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_` followed by 20 or more `[A-Za-z0-9]` | the token only → `<redacted>` |
   | 3 | URL userinfo | `logRedactURL` — a `://` (group 1), then the user, a `:`, then the password, then `@` | the `user:password` part only → `<redacted>`, keeping the scheme, the `@`, the host and the path — replace with `${1}<redacted>@` |
   | 4 | Bearer token | `logRedactBearer` — the word `Bearer` plus the following whitespace (group 1), then 8 or more non-whitespace characters | the token only → `<redacted>`, keeping the word `Bearer` — replace with `${1}<redacted>` |
   | 5 | Secret-named assignment | `logRedactAssign` — an identifier whose name contains a secret keyword, then `=` or `:`, then a value of `redactionMinSecretLength` or more non-whitespace characters; group 1 is the identifier plus the separator | the value run only → `<redacted>`, keeping the identifier and the separator — replace with `${1}<redacted>` |

   These exact patterns were compiled and run against all ten fixtures below, so treat the code block as authoritative over the prose cells:

   ```go
   var (
       logRedactPEM    = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`)
       logRedactGitHub = regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`)
       logRedactURL    = regexp.MustCompile(`(://)[^/\s@:]+:[^/\s@]+@`)
       logRedactBearer = regexp.MustCompile(`(?i)(Bearer\s+)\S{8,}`)
       logRedactAssign = regexp.MustCompile(
           `(?i)([A-Za-z0-9_.-]*(?:token|secret|passw|pem|dsn|credential|api[_-]?key|auth)[A-Za-z0-9_.-]*\s*[=:]\s*)\S{` +
               strconv.Itoa(redactionMinSecretLength) + `,}`,
       )
   )
   ```

   Apply them in that order, each with `ReplaceAllString` and the replacement string shown — the `${1}` is what keeps the identifier, the scheme and the word `Bearer`:

   | Pattern | Replacement |
   |---|---|
   | `logRedactPEM` | `<redacted>` |
   | `logRedactGitHub` | `<redacted>` |
   | `logRedactURL` | `${1}<redacted>@` |
   | `logRedactBearer` | `${1}<redacted>` |
   | `logRedactAssign` | `${1}<redacted>` |

   Class 5 deliberately does **not** fire on an ordinary logfmt key. `event=crash_arm_decision`, `task=service-analysis-needs-input` and `job=agent-task-executor-7f9c` must survive verbatim, or the captured tail is a wall of `<redacted>` and the feature defeats itself. Verified end to end: that exact logfmt line passes through unchanged, while `ANTHROPIC_AUTH_TOKEN=0123456789abc` becomes `ANTHROPIC_AUTH_TOKEN=<redacted>`.

   Compile every pattern once at package level with `regexp.MustCompile` — never inside `redactLog`, which runs on every failure publish.

   Document on `redactLog` that the residual risk is a credential shape this table does not recognise, and that adding a class means adding an entry to the table and a case to the test — never a tuning change to an existing pattern. Also document the accepted false positive: a key whose name merely *contains* a keyword is redacted even when it holds nothing secret (`author=`, `AUTH_URL=`, `token_count=`), because the alternative is a tail of `<redacted>`. That trade is deliberate.

4. **Create `pkg/log_redactor_internal_test.go`** (`package pkg`, internal, so it can reference `redactionMinSecretLength`). Use Ginkgo/Gomega v2. The top-level node MUST be `var _ = Describe("redactLog", func() { ... })` — the focused verification run selects on the word `redact`.

   Write exactly these specs:

   a. `DescribeTable("redacts each credential class", ...)` with five `Entry` rows, one per class in the table above. Each row passes a small input and the literal value that must disappear, and asserts both `Expect(redacted).NotTo(ContainSubstring(<literal>))` and `Expect(redacted).To(ContainSubstring("<redacted>"))`. Use these inputs so the classes cannot be satisfied by one shared shortcut:
      - PEM: `"-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j\n-----END RSA PRIVATE KEY-----\n"` — assert both `"BEGIN RSA PRIVATE KEY"` and `"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j"` are gone.
      - GitHub token: `"pushed with ghp_0123456789abcdefghijABCDEFGHIJ012345"` — assert the token literal is gone.
      - URL userinfo: `"clone https://deploy:hunter2hunter2@git.example.com/repo.git"` — assert `"hunter2hunter2"` is gone, `"git.example.com/repo.git"` survives, and `"https://"` survives (a bare `<redacted>` replacement drops the scheme and the `@`).
      - Bearer: `"Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"` — assert `"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"` is gone and `"Bearer"` survives.
      - Assignment: `"ANTHROPIC_AUTH_TOKEN=0123456789abc"` — assert `"0123456789abc"` is gone and `"ANTHROPIC_AUTH_TOKEN="` survives. The value is exactly `redactionMinSecretLength` characters, so a scrubber hardcoded to the four fixtures above fails this row; the key is one the pods actually carry.

   b. `It("leaves an ordinary log line unchanged", ...)` — input `"2026-11-03T10:00:00Z INFO starting worker ref=abc123\n"`, assert the output equals the input.

   c. `It("keeps the executor's logfmt fields readable", ...)` — input a realistic executor line, e.g. `"I1104 02:00:00.123456 1 watcher.go:42] event=crash_arm_decision task=service-analysis-needs-input job=agent-task-executor-7f9c reason=\"deadline_exceeded\"\n"`, assert `"event=crash_arm_decision"`, `"task=service-analysis-needs-input"` and `"job=agent-task-executor-7f9c"` all survive verbatim and that `"<redacted>"` does **not** appear. This is the regression pin for the design decision in requirement 3: a scrubber that redacts every long value fails here, which is exactly the shape that would make the captured tail useless.

   d. `It("keeps a value shorter than the floor readable", ...)` — build the value with `strings.Repeat("a", redactionMinSecretLength-1)`, wrap it as `"AUTH_TOKEN=" + value`, assert the output still contains that exact assignment. Reference the constant; never write `12` or `"aaaaaaaaaaaa"`.

   e. `It("redacts a value at exactly the floor", ...)` — build the value with `strings.Repeat("b", redactionMinSecretLength)`, wrap it as `"AUTH_TOKEN=" + value`, assert the output no longer contains the value and does contain `<redacted>`. Reference the constant; never write `13`.

   f. `It("redacts every class in one multi-line log", ...)` — one multi-line string carrying all five classes plus `"REF=abc123"` and an `event=` field, assert none of the five literals survive, `<redacted>` appears, and `"REF=abc123"` and the `event=` field survive. This is the shape the next change feeds to the scrubber, so it must hold on realistic mixed input.

5. Do **not** call `redactLog` from any production code path in this prompt. `pkg/result_publisher.go` and `pkg/job_watcher.go` stay untouched. The wiring is a separate change that depends on this one.

6. Before finishing, re-run `<verification>` and walk requirements 1-5 against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `ROOTDIR=/workspace make precommit` must exit 0.
- Do not modify `pkg/result_publisher.go`, `pkg/job_watcher.go`, `pkg/factory/factory.go`, `main.go`, or anything under `mocks/` in this prompt.
- Standard library only (`regexp`, `strconv`, `strings`). Do not add a module dependency and do not vendor a redaction library.
- No logging inside `redactLog`; it is a pure string function. No `context`, no error return, no I/O.
- Do not add a config field, an environment variable, a feature flag, or a Prometheus metric.
- Do not change the replacement placeholder from `<redacted>`; later changes assert on it.
- The redaction floor is the named constant `redactionMinSecretLength = 13`, applied to the value of an assignment whose key is secret-named (class 5). A value of 13 or more characters under such a key is redacted; a shorter one survives as a lookup key, and a value under an ordinary logfmt key (`event`, `task`, `job`, `reason`) is never redacted. Both the implementation and the boundary test reference the constant, never a literal.
- The redaction step must run on the raw text before any section string is composed — redacting an already-composed section is not acceptable. This prompt only provides the scrubber; the composing change must call it first.
- Ginkgo/Gomega v2 for the new specs. Spec names and the `Describe` name are load-bearing: the verification focuses on `redact`.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — exits 0.
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='redact' 2>&1 | tee /tmp/redact.out | grep -q 'Ran 10 of' && grep -q 'SUCCESS!' /tmp/redact.out` — exits 0 only when exactly ten focused specs ran (five table rows plus five `It` nodes) **and** all ten passed. Ginkgo prints `Ran N of M Specs` before the `SUCCESS!`/`FAIL!` line, so the count grep on its own would pass on a failing run. Do **not** put `--` before `-ginkgo.focus`: `go test` does not forward flags after `--`, so the flag is silently dropped, the whole 247-spec suite runs, and the count check is gone. A focus that matches no spec also exits 0, so the `Ran` count asserted by this command is the real check.
- `grep -n 'const redactionMinSecretLength = 13' pkg/log_redactor.go` — prints the line.
- `test "$(grep -c 'regexp.MustCompile' pkg/log_redactor.go)" -ge 5` — exits 0. Assert the threshold in the command: `grep -c` on its own exits 0 for any count from 1 to 4.
- `! grep -rn 'redactLog(' --include='*.go' pkg/ main.go | grep -v '_test.go' | grep -v 'pkg/log_redactor.go'` — exits 0; the wiring is a later change and no production file calls the scrubber here. The repo-wide form also catches a wiring made from a file other than the two obvious ones, and a stray comment naming `redactLog(` will fail it — remove the comment rather than the check.
</verification>
