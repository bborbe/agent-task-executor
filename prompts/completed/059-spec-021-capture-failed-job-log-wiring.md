---
status: completed
spec: [021-capture-failed-job-log-in-failure-section]
created: "2026-11-04T01:20:02Z"
queued: "2026-11-04T01:44:19Z"
completed: "2026-11-04T04:06:50Z"
---

# Write the failed pod's redacted log tail into the Failure section

<summary>
- Every failure record published for a failed Job whose task is still in the store now carries the tail of the failed pod's own log, written at the moment the failure is published; the store-miss and zombie-sweeper paths still write none
- The tail is read from the pod that failed, through the bounded reader added by the previous change
- Credential-shaped material is removed from the raw log before the failure section is built, so no unredacted value ever exists in the composed body
- A short lookup key stays readable, so the tail remains useful for triage
- The tail sits in a fenced block under a log-tail line, so the existing section writer still parses the record
- A log that cannot be read changes nothing: the failure still publishes, no block is added, and one log line names the job
- A successful Job reads no log at all
- The failure-publish method keeps its existing signature, so every caller is unchanged
- The pod is now resolved for every failed Job, not only for deadline kills, because the crash class is exactly where the log matters most
- The change is recorded in the changelog
</summary>

<objective>
Complete the feature: a failed Job's `## Failure` section carries a bounded, redacted tail of its own pod log, captured at publish time, so diagnosis never depends on the pod outliving the failure — while a log that cannot be read leaves the failure record exactly as it is today.
</objective>

<context>
Read these files in full before changing anything:

- `pkg/result_publisher.go` — `publishFailure` is the single composer of the `## Failure` section (`fmt.Sprintf("## Failure\n\n- **Timestamp:** %s\n- **Job:** %s\n- **Reason:** %s\n%s", ...)`). `PublishFailure` and `PublishStoreMissFailure` are thin wrappers over it. `NewResultPublisher` builds the concrete type. The `ResultPublisher` interface is frozen for `PublishFailure` by spec 004.
- `pkg/pod_log_reader.go` — the `PodLogReader` interface and `NewPodLogReader(client)`, added by the previous change. `Tail(ctx, namespace, podName) (string, error)` is bounded at the API.
- `pkg/log_redactor.go` — `redactLog(text string) string` and `redactionMinSecretLength`, added by the first change.
- `pkg/job_watcher.go` — `publishSyntheticFailure` currently resolves the pod only for the deadline class (`if JobFailureReason(job) == ZombieReasonDeadlineExceeded { pod = w.jobPod(job) }`), with a comment explaining that no other class used it. That premise is now false.
- `pkg/result_publisher_test.go` — `captureGlogStderr`, `capturingSyncProducer`, `decodeUpdateFrontmatterCommand`, and the existing `Describe("PublishFailure", ...)` block. The decoded `taskcmd.UpdateFrontmatterCommand` exposes `Body *taskcmd.BodySection` with `Heading` and `Section` fields.
- `pkg/job_watcher_internal_test.go` — `Describe("HandleJob with resolvable pod lister", ...)`, `makeJob`, `backoffCondition`, `makeOwnedTerminatedPod`, `setPodLister`, and `fakePublisher.PublishFailurePodArgsForCall`.
- `main.go` — `kubeClient` is created before `pkg.NewResultPublisher` is called; wire the reader there.

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-glog-guide.md` — `V(1)` and the `event=` convention the surrounding lines follow
- `/home/node/.claude/plugins/marketplaces/coding/docs/changelog-guide.md` — `## Unreleased` bullet format

Verified facts (do not re-derive):
- The current `PublishFailure` signature is `PublishFailure(ctx context.Context, task lib.Task, jobName string, reason string, pod *corev1.Pod) error` — five parameters. It must stay exactly that.
- `NewResultPublisher` is called in `main.go:370`, `pkg/result_publisher_topic_test.go:42`, `pkg/handler/task_event_handler_trigger_refusal_test.go:109`, and ten places in `pkg/result_publisher_test.go` (the shared `BeforeEach` plus nine local publishers). Use `grep -rn 'NewResultPublisher(' --include='*.go' .` as the checklist and let the compiler confirm.
- `pkg/suite_test.go` defines `TestPkg`; every Ginkgo spec runs under it, so a `-run '<SpecName>'` filter selects nothing and exits 0.
- `CHANGELOG.md` currently starts with `## v0.55.2`; there is no `## Unreleased` section.
- `pkg/job_watcher_internal_test.go` and `pkg/result_publisher_test.go` both live in one test binary, so their specs run together under `TestPkg`.
</context>

<requirements>
1. **Add the reader to the publisher.** In `pkg/result_publisher.go`:

   - add `podLogReader PodLogReader` to the `resultPublisher` struct, documented as: nil means log capture is off, and every publish then behaves exactly as before.
   - add a fifth parameter to `NewResultPublisher` after `claimRefClientFactory`:

     ```go
     func NewResultPublisher(
     	syncProducer libkafka.SyncProducer,
     	topicPrefix base.TopicPrefix,
     	currentDateTime libtime.CurrentDateTimeGetter,
     	claimRefClientFactory ClaimRefClientFactory,
     	podLogReader PodLogReader,
     ) ResultPublisher
     ```

     Document on the constructor that `podLogReader` may be nil, in which case no log tail is captured.
   - add the import `"strings"`.

2. **Add the tail-block helper** to `pkg/result_publisher.go`, next to `publishFailure`:

   ```go
   // logTailBlock returns the fenced `- **Log tail:**` block for the failed pod's
   // log, or "" when there is no tail to write.
   //
   // The raw log is redacted HERE, before the section string is composed: an
   // unredacted value must never have a moment where it could be logged, returned
   // or written. A nil reader (log capture off) or a nil pod (a failure class that
   // carries none) yields no block. A read failure is logged at V(1) and
   // swallowed — the failure record is the point, and losing it to a logging
   // failure would be the worse outcome.
   func (p *resultPublisher) logTailBlock(
   	ctx context.Context,
   	task lib.Task,
   	jobName string,
   	pod *corev1.Pod,
   ) string {
   	if p.podLogReader == nil || pod == nil {
   		return ""
   	}
   	tail, err := p.podLogReader.Tail(ctx, pod.Namespace, pod.Name)
   	if err != nil {
   		glog.V(1).Infof(
   			"event=log_tail_unavailable task=%s job=%s pod=%s/%s error=%v",
   			task.TaskIdentifier, jobName, pod.Namespace, pod.Name, err,
   		)
   		return ""
   	}
   	tail = strings.TrimRight(redactLog(tail), "\n")
   	if tail == "" {
   		return ""
   	}
   	return "\n- **Log tail:**\n\n```\n" + tail + "\n```\n"
   }
   ```

   The exact rendered block is `- **Log tail:**`, a blank line, an opening fence, the tail text, a closing fence — one opening fence, immediately before the tail text. Later specs assert that literal shape, so do not reorder it, do not add a blank line between the fence and the text, and do not add a heading of its own.

3. **Compose the tail into the section.** In `publishFailure`, after the existing dedupe check and after `now` is computed, build the section with one extra `%s`:

   ```go
   	section := fmt.Sprintf(
   		"## Failure\n\n- **Timestamp:** %s\n- **Job:** %s\n- **Reason:** %s\n%s%s",
   		now,
   		jobName,
   		reason,
   		note,
   		p.logTailBlock(ctx, task, jobName, pod),
   	)
   ```

   The dedupe check stays first, so a repeated informer delivery does not read the log again. The `note` keeps its position immediately after the reason line. `PublishStoreMissFailure` continues to pass a nil pod, so it writes no tail; state that in one comment sentence at its `publishFailure` call.

4. **Resolve the pod for every failed Job.** In `pkg/job_watcher.go`, `publishSyntheticFailure` currently resolves the pod only when `JobFailureReason(job) == ZombieReasonDeadlineExceeded`. Replace that conditional with an unconditional `pod := w.jobPod(job)` and rewrite the comment: the pod now feeds two consumers — the never-scheduled arm gate and the log tail — and the log matters most for the crash class, which is exactly the class the old condition excluded. Keep `jobPod`'s informer-backed lookup: the constraint is that the LOG READ does not depend on the informer, not that pod selection may not use it. `HandlePod` already passes its own pod and needs no change. Also update `ResultPublisher.PublishFailure`'s doc comment in `pkg/result_publisher.go`: its paragraph saying `pod` "is used only to discriminate the never-scheduled deadline class at the arm gate" is now false — `pod` also feeds the log tail.

5. **Update every `NewResultPublisher` call site.** Add `nil` as the fifth argument in `pkg/result_publisher_topic_test.go`, `pkg/handler/task_event_handler_trigger_refusal_test.go`, and each local publisher in `pkg/result_publisher_test.go` (leave the shared `BeforeEach` at `nil`; the new specs build their own publisher). In `main.go`, pass `pkg.NewPodLogReader(kubeClient)` **inline** as the fifth argument to the `NewResultPublisher(` call — do not extract it to a local variable first. The verification scopes a `sed` range around that call and counts the constructor inside it, so an extracted variable would print `0` and read as an unwired feature.

6. **Add the composed-body specs** to `pkg/result_publisher_test.go`. Add the imports `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"` if you use `ObjectMeta` (the file already imports `corev1` and `stderrors`). Build each publisher locally with a `&mocks.FakePodLogReader{}` and this pod fixture:

   ```go
   logPod := &corev1.Pod{
   	ObjectMeta: metav1.ObjectMeta{Namespace: "agent", Name: "job-1-abcde"},
   }
   ```

   Use a task `lib.Task{TaskIdentifier: "test-task-log-tail", Frontmatter: lib.TaskFrontmatter{"max_triggers": 3}}` and reason `"pod_crash_no_stdout"` so the arm gate is inert and the assertions are about the body alone.

   a. `Describe("PublishFailure redaction in the composed body", ...)` — the name contains `redact` on purpose; the verification focuses on it.
      - `It("redacts every credential shape from the composed failure body", ...)` — the reader returns a log containing all five shapes plus a lookup key:

        ```go
        "2026-11-03T10:00:00Z INFO starting worker ref=abc123\n" +
        "pushed with ghp_0123456789abcdefghijABCDEFGHIJ012345\n" +
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln\n" +
        "clone https://deploy:hunter2hunter2@git.example.com/repo.git\n" +
        "ANTHROPIC_AUTH_TOKEN=zz99887766554\n" +
        "-----BEGIN RSA PRIVATE KEY-----\n" +
        "MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j\n" +
        "-----END RSA PRIVATE KEY-----\n"
        ```

        Call `publisher.PublishFailure(ctx, task, "job-1", "pod_crash_no_stdout", logPod)`, decode `producer.messages[0]`, and assert against `updateCmd.Body.Section` — never against the reader's return value and never against the redactor's return value. Assert `NotTo(ContainSubstring(...))` for each of: `"ghp_0123456789abcdefghijABCDEFGHIJ012345"`, `"BEGIN RSA PRIVATE KEY"`, `"MIIEowIBAAKCAQEA1a2b3c4d5e6f7g8h9i0j"`, `"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2ln"`, `"hunter2hunter2"`, `"zz99887766554"`. Also assert `ContainSubstring("<redacted>")`. The 13-character fixture sits under a secret-named key (`ANTHROPIC_AUTH_TOKEN=`) because that is the only shape class 5 redacts — under a neutral key such as `payload=` the value is left readable by design, and asserting its absence there would fail against a correct redactor. The value `zz99887766554` is used in no other spec. A perfect redactor wired to nothing, and a reader never called, both fail this spec.
      - `It("keeps a short lookup key readable in the composed body", ...)` — the reader returns `"2026-11-03T10:00:00Z INFO lookup ref=abc123\n"`; assert `updateCmd.Body.Section` contains `"ref=abc123"` verbatim.
      - `It("composes the tail as one fenced block", ...)` — the reader returns `"line one\nline two\n"`; assert `updateCmd.Body.Section` contains `"- **Log tail:**\n\n```\nline one\nline two\n```\n"` and `strings.Count(updateCmd.Body.Section, "```")` is `2`.

   b. `Describe("PublishFailure log tail read failures", ...)`
      - `It("log read fails: still publishes the failure without a log block", ...)` — `reader.TailReturns("", stderrors.New("pods \"job-1-abcde\" not found"))`. Wrap the publish in `captureGlogStderr`. Assert the returned error is nil, `updateCmd.Body.Section` contains `"## Failure"` and does not contain `"Log tail"`, and the captured output contains exactly one `"event=log_tail_unavailable "` line naming the job: `strings.Count(out, "event=log_tail_unavailable ")` is `1` and `out` contains `"job=job-1"`.
      - `It("reads no log on the success path", ...)` — use the same reader-wired publisher as the specs above, **not** the shared `BeforeEach` one whose reader is nil, and call `publisher.PublishClearCurrentJob(ctx, task, "job-success")`; assert `reader.TailCallCount()` is `0`, `producer.messages` has one message, and its decoded `cmd.Body` is nil. A succeeded Job never reaches the failure path, so a reader wired into a shared helper instead of the failure path fails here.
      - `It("writes no log block when the failure carries no pod", ...)` — on the same reader-wired publisher, call `publisher.PublishFailure(ctx, task, "job-nopod", "deadline_exceeded", nil)`; assert `reader.TailCallCount()` is `0` and `updateCmd.Body.Section` contains no `"Log tail"`. This is the zombie-sweeper production path: a real reader is wired and the pod is nil.

7. **Add the watcher-level spec** to the `Describe("HandleJob with resolvable pod lister", ...)` block in `pkg/job_watcher_internal_test.go`:

   `It("passes the owned pod to PublishFailure for a backoff-limit failure", ...)` — store `testTask`, build `makeJob("job-backoff-pod", string(testTaskID), backoffCondition())`, seed the lister with `makeOwnedTerminatedPod("pod-backoff", "job-backoff-pod", string(testTaskID), "Error", 1, "2026-08-16T00:00:00Z")`, call `watcher.HandleJob(ctx, job)`, and assert `fakePublisher.PublishFailurePodArgsForCall(0)` is not nil and carries name `pod-backoff`. Without this spec the pod-resolution change in requirement 4 could be reverted and the suite would stay green while the crash class silently captured no log.

8. **CHANGELOG.** Insert an `## Unreleased` section directly below the `All notable changes...` paragraph and above `## v0.55.2`, with exactly one bullet:

   ```markdown
   ## Unreleased

   - feat: **a failed Job's `## Failure` section now carries a redacted, bounded tail of the pod's own log, captured at publish time.** Pods survive only `ttlSecondsAfterFinished: 1800`, so a diagnosis that lands later found the pod already gone and the record naming *that* the Job failed but never *why*. The tail is read through the Kubernetes log API with `TailLines` and `LimitBytes` applied at the request, redacted before the section string is composed (GitHub tokens, PEM private-key blocks, bearer tokens, URL userinfo, and any assignment of `redactionMinSecretLength` = 13 or more characters; shorter lookup keys survive), and fenced so the existing body-section writer still parses the section. A read failure — the pod collected, the `pods/log` grant missing, throttling — is logged on one V(1) line and swallowed: the failure still publishes and `PublishFailure` keeps its signature. A successful Job reads no log. The `pods/log` RBAC grant is a companion change in `bborbe/agent`.
   ```

9. **Run the verification and fix what it reports.** Before finishing, walk requirements 1-8 against the diff and confirm the frozen signature is intact.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `ROOTDIR=/workspace make precommit` must exit 0.
- `PublishFailure(ctx, task, jobName, reason, pod)` keeps its exact signature. Do not add a parameter, do not return the tail, do not change `PublishStoreMissFailure`'s signature either.
- The redaction step must run before the section string is built; redacting the already-composed string is not acceptable. `logTailBlock` is the only place the raw log is touched.
- A log-read error is logged and swallowed. It must never be returned, never wrapped into the publish error, and never abort the publish. The failure record is the primary output.
- A successful Job reads no log: the reader is called only from `logTailBlock`, which is reachable only from `publishFailure`.
- Do not depend on the job watcher's informer cache for the log read. The reader takes a client and a namespace; the publisher holds only the `PodLogReader` interface.
- No new cluster permission beyond reading pod logs in the namespaces the executor already watches. The `pods/log` subresource grant lives in `bborbe/agent` (`helm/templates/executor-rbac.yaml`), not in this repo — do not add a Role, a ClusterRole, a manifest, or an RBAC test here.
- Do not change `ttlSecondsAfterFinished`, add a log-shipping pipeline, or capture logs for Jobs that succeed.
- Do not add a config field, an environment variable, a feature flag, or a Prometheus metric.
- Keep `publishFailure`'s existing error wrapping and the caller-side `glog.Errorf` behaviour unchanged.
- Ginkgo/Gomega v2 for new specs. Never hand-edit `mocks/`; run `ROOTDIR=/workspace make generate` if a mock needs regenerating.
- This change requires prompts 1 and 2 of the same spec to be merged into the branch first: it imports `redactLog`, `redactionMinSecretLength`, `PodLogReader`, `NewPodLogReader` and `mocks.FakePodLogReader`, none of which exist before they land. If any of them is missing, stop and report rather than reimplementing it here.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — exits 0.
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='PublishFailure redact' 2>&1 | tee /tmp/wire1.out | grep -q 'Ran 3 of' && grep -q 'SUCCESS!' /tmp/wire1.out` — exits 0 only when exactly three composed-body specs ran (including the `lookup key` one) **and** all three passed. Ginkgo prints `Ran N of M Specs` before `SUCCESS!`/`FAIL!`, so the count grep alone would pass on a failing run. The bare `-ginkgo.focus='redact'` would also select the redactor unit specs from the first change, so the qualified focus is what makes the count exact. Do **not** put `--` before `-ginkgo.focus`: `go test` does not forward flags after `--`, so the flag is silently dropped, the whole 247-spec suite runs, and the count check is gone. A focus matching no spec also exits 0, so the `Ran` count asserted by this command is the real check.
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='log tail' 2>&1 | tee /tmp/wire2.out | grep -q 'Ran 3 of' && grep -q 'SUCCESS!' /tmp/wire2.out` — exits 0 only when exactly three specs ran (log-read-failure, success-path, no-pod) **and** all three passed.
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='owned pod to PublishFailure' 2>&1 | tee /tmp/wire3.out | grep -q 'Ran 1 of' && grep -q 'SUCCESS!' /tmp/wire3.out` — exits 0 only when exactly one spec ran (the watcher spec) **and** it passed.
- `sed -n '/^\tPublishFailure($/,/^\t) error$/p' pkg/result_publisher.go | grep -cE '^\t\t[a-z]'` — prints exactly `5`, one line per parameter, so an added or removed parameter changes the number. Do not match on the five known parameter types: that form prints `5` even after a sixth parameter is added, so it can only catch a removal.
- `sed -n '/resultPublisher := pkg.NewResultPublisher(/,/^\t)/p' main.go | grep -c 'pkg.NewPodLogReader(kubeClient)'` — prints `1`: the reader is passed **into** the constructor, not merely constructed somewhere in the file. A bare `grep -n 'NewPodLogReader(kubeClient)' main.go` also passes when the reader is assigned to an unused variable while `nil` is passed to the publisher, which ships the feature silently off.
- `awk '/^## /{sec=$0} /bounded tail of the pod/{print sec}' CHANGELOG.md | grep -q '^## Unreleased'` — exits 0: the entry is present and sits under the unreleased heading.
</verification>

<!-- OPEN QUESTION (for the spec reviewer, not for the executing agent):
     A store-miss failure and a zombie-sweeper failure pass no pod, so their
     `## Failure` sections carry no log tail. The store-miss path does have the
     failed Job (`publishStoreMissFailure` receives it) and could resolve the pod
     with `w.jobPod(job)`; carrying it through would mean adding a parameter to
     `PublishStoreMissFailure`, which none of the spec's acceptance criteria ask
     for. This change leaves both paths without a tail on purpose. If the
     reviewer wants store-miss failures to carry a tail too, that is a follow-up
     change to `PublishStoreMissFailure`'s signature and the two call sites.
-->

<!-- DECISION (for the reviewer, not for the executing agent — do not act on it):
     The spec bounds the read by line count and byte count but names no time
     bound. The previous change added `logTailReadTimeout = 10s` inside the
     reader so a hung log stream cannot stall a failure publish indefinitely,
     motivated by the spec's own invariant that a delivered failure must never
     become an error. That constant is part of this change's contract and is
     verified by a grep in prompt 2. Do not remove it.
-->

<!-- NOTE (for the reviewer — do not act on it):
     `pkg/` has no V(1) call today (23 x V(2), 4 x V(3)), and the deployed
     executor's `-v` level is unverified. The spec mandates the
     `event=log_tail_unavailable` line at V(1); the spec's own test captures it
     by raising v to 2 via `captureGlogStderr`. If the deployed executor runs
     at v=0 that line is invisible in production even though the test passes.
     Raising it to V(2) would match the package's floor, but that is a spec
     change, not a prompt change, so it is recorded here rather than applied.
-->
