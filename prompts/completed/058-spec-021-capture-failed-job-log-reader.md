---
status: completed
spec: [021-capture-failed-job-log-in-failure-section]
created: "2026-11-04T01:20:02Z"
queued: "2026-11-04T01:44:19Z"
completed: "2026-11-04T04:06:50Z"
---

# Read a bounded pod log tail through the Kubernetes API

<summary>
- A small component can read one pod's log without any informer cache, so it can be used from the publish path independently of the Job watcher
- The read asks the API for at most a fixed number of lines and a fixed number of bytes, so a runaway log cannot reach the caller
- The caller also reads at most the byte bound from the stream, so a response that ignores the limit cannot blow up memory
- A single read is time-boxed, so a hung log stream cannot stall a failure publish forever
- A refusal from the API — the pod already collected, a permission error, throttling — comes back as an error for the caller to swallow
- The component is an interface with a generated fake, so the publish path can be tested with a call-counting reader
- Nothing is wired into the publish path yet
</summary>

<objective>
Add a bounded, cache-independent pod-log reader to the `pkg` package so the failure publish path can obtain a failed Job's log tail at publish time, limited at the API by line count and byte count, with a single read failure surfaced as an error the caller can swallow.
</objective>

<context>
Read these files before changing anything:

- `pkg/job_watcher.go` — `jobPod`, `terminalPodDetail` and `ownerJobName` show how the watcher resolves a Job's pod today. This reader deliberately does NOT reuse the informer-backed `PodLister`: the publisher and the watcher are constructed independently, so the reader takes a `kubernetes.Interface` and reads the log API directly.
- `pkg/result_publisher.go` — the publisher that will consume this reader in a later change. Read `NewResultPublisher` and the `ResultPublisher` interface. Do not modify this file in this prompt.
- `pkg/job_watcher_test.go` and `pkg/job_watcher_success_before_sync_test.go` — the repo's existing `fake.NewClientset()` usage. (several older files still use `fake.NewSimpleClientset()`, e.g. `pkg/zombie_sweeper_test.go`, `pkg/job_watcher_internal_test.go`, `pkg/k8s_connector_test.go` — follow the newer `fake.NewClientset()` form.)
- `pkg/k8s_connector_test.go` — the repo's existing `k8stesting "k8s.io/client-go/testing"` usage, including how a reactor is registered and how a recorded action is read back.
- `pkg/result_publisher.go` line with `//counterfeiter:generate` — the counterfeiter directive convention (`-o ../mocks/<file>.go --fake-name Fake<Name> . <Interface>`).

Coding-plugin guides (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega v2 conventions
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-mocking-guide.md` — counterfeiter directive and fake usage
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — `errors.Wrapf(ctx, err, ...)`, never `fmt.Errorf`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md` — bounding one external call with a timeout

Verified facts (do not re-derive):
- `k8s.io/client-go@v0.37.1` fake pods implement `GetLogs` (`kubernetes/typed/core/v1/fake/fake_pod_expansion.go`): it builds a `GenericAction` with `Verb: "get"`, `Resource: "pods"`, `Subresource: "log"`, and `Value: *corev1.PodLogOptions`, invokes the fake's reactors, and serves the returned `*runtime.Unknown.Raw` as the HTTP body. With no reactor it serves the literal `fake logs`. The action carries **no pod name**: `GenericActionImpl` has no name field and `k8stesting.Action` has no `GetName()`. Assert the pod name through the wrapped error message, never through the action.
- A reactor registered with `PrependReactor("get", "pods", ...)` can read the options via `action.(k8stesting.GenericAction).GetValue().(*corev1.PodLogOptions)`, and can return `(true, &runtime.Unknown{Raw: []byte(body)}, nil)` or `(true, nil, err)`.
- `corev1.PodLogOptions` has `TailLines *int64` and `LimitBytes *int64`.
- `fake.NewClientset()` is the constructor used in this repo's newer tests (`pkg/job_watcher_success_before_sync_test.go`).
- The `pkg` package already carries a counterfeiter `-generate` directive, so a new `//counterfeiter:generate` line in this package is picked up by `ROOTDIR=/workspace make generate`.
</context>

<requirements>
1. **Create `pkg/pod_log_reader.go`** (`package pkg`) with the BSD license header, three bounds, the interface, the directive, and the implementation.

2. **Declare the bounds as package constants:**

   ```go
   const (
   	// logTailMaxLines bounds the number of trailing lines requested from the log API.
   	logTailMaxLines = 200
   	// logTailMaxBytes bounds the bytes requested from the log API and the bytes
   	// read from the stream. Both halves are needed: the API limit is what keeps a
   	// runaway log out of the response, and the stream limit keeps a response that
   	// ignores it from being buffered whole.
   	logTailMaxBytes = 64 * 1024
   	// logTailReadTimeout bounds one log read. A hung stream must not stall a
   	// failure publish: the publish is the record, and a read is only evidence.
   	logTailReadTimeout = 10 * time.Second
   )
   ```

3. **Declare the interface and its fake directive** above the implementation:

   ```go
   //counterfeiter:generate -o ../mocks/pod_log_reader.go --fake-name FakePodLogReader . PodLogReader

   // PodLogReader reads the tail of one pod's log directly from the Kubernetes API.
   // It takes a client and a namespace per call and holds no informer cache, so it
   // can be constructed independently of the Job watcher.
   type PodLogReader interface {
   	// Tail returns the pod's trailing log text, bounded by logTailMaxLines and
   	// logTailMaxBytes. It returns an error when the API refuses the read — the
   	// pod is already garbage-collected, the caller lacks the pods/log grant, or
   	// the API throttles — and the caller is expected to swallow that error: a
   	// missing tail must never turn a delivered failure into an error.
   	Tail(ctx context.Context, namespace string, podName string) (string, error)
   }
   ```

4. **Implement the constructor and the reader:**

   ```go
   func NewPodLogReader(client kubernetes.Interface) PodLogReader
   ```

   The concrete type holds only the client. `Tail` must:
   - take `tailLines := int64(logTailMaxLines)` and `limitBytes := int64(logTailMaxBytes)`;
   - bound the whole read with `context.WithTimeout(ctx, logTailReadTimeout)` and cancel it with `defer`;
   - call `client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailLines, LimitBytes: &limitBytes})` — both fields set on the request, because that is where the bound is actually applied;
   - call `.Stream(readCtx)`, assign the result to a variable named `stream`, and wrap a failure with `errors.Wrapf(ctx, err, "stream log for pod %s/%s", namespace, podName)`;
   - close the stream with a deferred close whose error is discarded explicitly (`defer func() { _ = stream.Close() }()`);
   - read with `io.ReadAll(io.LimitReader(stream, limitBytes))` and wrap a read failure the same way;
   - return the text unchanged, including its trailing newline. Do not trim, do not join lines, do not prefix anything.

   Document on the type that the line and byte limits are applied at the API and that the stream limit is the second half of the same bound, not a post-hoc truncation of an already-buffered log.

5. **Create `pkg/pod_log_reader_internal_test.go`** (`package pkg`, so it can reference the three constants). Ginkgo/Gomega v2, top-level node `var _ = Describe("podLogReader", func() { ... })` — the focused verification selects on the word `podLogReader`. Write exactly these four specs, each building a `fake.NewClientset()` and registering a `PrependReactor("get", "pods", ...)` that ignores non-`log` subresources by returning `false, nil, nil`:

   a. `It("requests a line-limited and byte-limited tail", ...)` — reactor captures the options and `action.GetNamespace()` and returns `"hello\n"`. Assert `*captured.TailLines == int64(logTailMaxLines)`, `*captured.LimitBytes == int64(logTailMaxBytes)`, that the captured namespace equals the namespace passed to `Tail`, and that the returned text is `"hello\n"`. Do **not** assert `action.GetName()`: the fake builds a `GenericActionImpl`, which carries no name field and does not implement `k8stesting.GetAction`, so `action.GetName()` does not compile and `action.(k8stesting.GetAction)` yields `ok == false`. The pod name is observable only through the wrapped error, asserted in spec d. Without the namespace assertion a reader that ignores its own namespace argument still passes.

   b. `It("returns at most the byte limit for an over-limit log", ...)` — reactor returns `strings.Repeat("x", logTailMaxBytes*10)`. Assert `len(tail)` is `BeNumerically("<=", logTailMaxBytes)` and the error is nil. The fake does not enforce `LimitBytes`, so this spec fails a reader that buffers the response whole.

   c. `It("returns the log text unchanged", ...)` — reactor returns a two-line body ending in a newline; assert the result equals it byte-for-byte.

   d. `It("returns the API error", ...)` — reactor returns `(true, nil, stderrors.New("pods \"job-1-abcde\" not found"))`; assert the returned text is empty, the error has occurred, and the error message contains `<namespace>/<podName>` — the wrapped error is the only place the pod name passed to `Tail` is observable, because the fake's `GenericActionImpl` does not carry it. Use `stderrors "errors"` as the import alias, matching `pkg/result_publisher_test.go`.

   Assert the captured options via a pointer field on the spec closure, not by inspecting `fakeClient.Actions()` — the log action carries the options in `Value`, and the reactor is the supported place to read them.

6. **Generate the fake.** Run `ROOTDIR=/workspace make generate`. Confirm `mocks/pod_log_reader.go` exists and its `FakePodLogReader` exposes `TailCallCount()`, `TailArgsForCall(int) (context.Context, string, string)`, and `TailReturns(string, error)`. Never hand-edit `mocks/`.

7. Do **not** wire the reader into `pkg/result_publisher.go`, `pkg/factory/factory.go`, or `main.go` in this prompt. That is the next change, which depends on this one.

8. Before finishing, re-run `<verification>` and walk requirements 1-7 against the diff.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass; `ROOTDIR=/workspace make precommit` must exit 0.
- Do not modify `pkg/result_publisher.go`, `pkg/job_watcher.go`, `pkg/factory/factory.go`, `main.go`, `pkg/log_redactor.go`, or any file under `mocks/` by hand.
- The reader must not take a `corev1listers.PodLister` or any informer-backed accessor. It takes a `kubernetes.Interface` and reads the log API directly.
- The read must be bounded at the API with `PodLogOptions{TailLines, LimitBytes}` — never by reading a whole log and slicing it afterwards. The stream-level `io.LimitReader` is an additional bound on the same number, not a replacement for the request fields.
- Errors: `errors.Wrapf(ctx, err, ...)` from `github.com/bborbe/errors`; never `fmt.Errorf`. Do not log inside the reader; the caller owns the log line.
- No new cluster permission is added by this change. Reading pod logs is the `pods/log` subresource, whose grant lives in `bborbe/agent` (chart RBAC), not in this repo. Do not add a Role, a ClusterRole, a manifest, or an RBAC test here.
- Do not add a config field, an environment variable, a feature flag, or a Prometheus metric. The three bounds are package constants.
- Do not add retries, backoff, or a fallback to the informer cache. One attempt, bounded, error out.
- Ginkgo/Gomega v2 for the new specs.
</constraints>

<verification>
- `ROOTDIR=/workspace make precommit` — exits 0.
- `go test ./pkg/ -run TestPkg -v -args -ginkgo.focus='podLogReader' 2>&1 | tee /tmp/podlog.out | grep -q 'Ran 4 of' && grep -q 'SUCCESS!' /tmp/podlog.out` — exits 0 only when exactly four focused specs ran **and** all four passed. Ginkgo prints `Ran N of M Specs` before the `SUCCESS!`/`FAIL!` line, so the count grep on its own would pass on a failing run. Do **not** put `--` before `-ginkgo.focus`: `go test` does not forward flags after `--`, so the flag is silently dropped, the whole suite runs, and the count check is gone.
- `grep -n 'TailLines:' pkg/pod_log_reader.go` — prints the line (the line limit is on the request).
- `grep -n 'LimitBytes:' pkg/pod_log_reader.go` — prints the line (the byte limit is on the request).
- `grep -n 'context.WithTimeout(ctx, logTailReadTimeout)' pkg/pod_log_reader.go` — prints the line (the timeout context exists). No spec exercises the timeout: the fake serves a `bytes.Reader` that never blocks, so a real timeout test needs a hand-rolled client. These greps are the only check, which is why there are two of them.
- `grep -n 'Stream(readCtx)' pkg/pod_log_reader.go` — prints the line: the timeout context is the one that bounds the stream. The `WithTimeout` grep alone passes even when its result is discarded (`_, cancel := context.WithTimeout(...)` then `.Stream(ctx)`).
- `grep -n 'io.LimitReader(stream' pkg/pod_log_reader.go` — prints the line (the stream is bounded too).
- `grep -n 'func (fake \*FakePodLogReader) TailCallCount' mocks/pod_log_reader.go` — prints the generated accessor.
- `grep -n 'func NewPodLogReader(client kubernetes.Interface)' pkg/pod_log_reader.go` — prints the constructor taking a client, not a lister.
- `test -f pkg/pod_log_reader.go && ! grep -q 'corev1listers' pkg/pod_log_reader.go` — exits 0 only when the file exists and has no informer dependency (a bare `! grep -q` also passes when the file is missing, since grep exits 2).
</verification>
