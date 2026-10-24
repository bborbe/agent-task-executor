---
status: draft
spec: [016-bug-claim-only-until-quota]
created: "2026-10-24T19:34:00Z"
branch: dark-factory/bug-claim-only-until-quota
---

<summary>
- A new read-only component reports how many pods the namespace `ResourceQuota` allows for one agent priority class.
- The quota is selected by its `PriorityClass In [<name>]` scope selector, not by the quota's object name.
- The reported limit is `hard.pods`, preferring the quota's `status.hard` and falling back to `spec.hard`.
- An absent priority class, no matching quota, or a non-positive limit reports "no quota bound" instead of an error.
- Two quotas matching the same priority class report the smaller limit and log one warning naming both.
- Reads are cached for 30 seconds per namespace and priority class, so admission does not pay a list call per task.
- A failed or forbidden read returns an error and is never cached, so the next admission retries.
- Nothing uses the new reader yet; the next change wires it into admission.
- No behaviour changes anywhere: nothing calls the new component until the next prompt wires it in.
</summary>

<objective>
Add a `PodQuotaLimiter` to `pkg/spawner` that answers "how many pods may this agent's priority class run in this namespace" from the namespace `ResourceQuota`, with a 30 s cache and a table-tested selector. This is the enforced capacity the next prompt uses as an admission bound; on its own it changes nothing.
</objective>

<context>
Read `docs/claim-lease-protocol.md` (§ Constants, § Claim state machine) for the protocol vocabulary this package's comments use.

Read these files before writing code:
- `pkg/spawner/job_spawner.go` — the package's interface + `New*` constructor + private struct pattern, its `k8s "github.com/bborbe/k8s"` alias (spelled `libk8s` in `pkg/handler/job_cache.go` and `job_namespace.go`), and its `errors.Wrapf(ctx, err, "...")` style.
- `pkg/spawner/spawner_suite_test.go` — the Ginkgo entry point and the `//go:generate` directive for this package.
- `pkg/spawner/job_spawner_test.go` — how this package's tests build `fake.NewClientset()`, set the injected clock with `currentDateTime.SetNow(libtimetest.ParseDateTime("..."))`, and assert with Gomega.
- `pkg/handler/task_event_handler_pin_outside_pool_test.go` — `capturePinReportLog`, the glog capture pattern (`flag.Set("logtostderr", "true")`, `os.Stderr` pipe, `glog.Flush()`). Copy that shape into the spawner test file; it is the only existing log-capture exemplar.
- `pkg/handler/job_cache.go` — the existing 10 s / generation-keyed cache in this codebase; the new quota cache follows the same "read at most once per window" reasoning at 30 s (spec DB 5), but lives in the reader, not on the handler.

Verified library facts (already grepped from the module cache — use these exact symbols):
- `corev1 "k8s.io/api/core/v1"` provides `ResourceQuota`, `ResourceQuotaList`, `ResourceQuotaScopePriorityClass` (value `"PriorityClass"`), `ScopeSelectorOpIn` (value `"In"`), `ScopedResourceSelectorRequirement{ScopeName, Operator, Values}`, `ResourceQuota.Spec.ScopeSelector *ScopeSelector` with `MatchExpressions []ScopedResourceSelectorRequirement`, `ResourceQuota.Status.Hard ResourceList` and `ResourceQuota.Spec.Hard ResourceList`, and `ResourcePods ResourceName = "pods"`.
- `ResourceList` is `map[ResourceName]resource.Quantity`; `quantity.Value() int64` is the integer value.
- `kubernetes.Interface.CoreV1().ResourceQuotas(namespace string).List(ctx, metav1.ListOptions{})` returns `(*corev1.ResourceQuotaList, error)`.
- `libtime.CurrentDateTimeGetter` has exactly one method: `Now() libtime.DateTime`; `libtime.DateTime` has `Time() time.Time`.
- The fake clientset records every action: `fakeClient.Actions()` returns `[]k8stesting.Action`, and `action.Matches(verb, resource)` answers `Matches("list", "resourcequotas")`.
- Reactors: `fakeClient.PrependReactor("list", "resourcequotas", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {...})` — return `(true, nil, err)` to inject a failure; `(false, nil, nil)` falls through to the object tracker.
</context>

<requirements>
1. Create `pkg/spawner/pod_quota_limiter.go` with this interface and constructor. Copy the doc-comment style from `JobSpawner` in `pkg/spawner/job_spawner.go`: every exported name has a comment that starts with the name and explains the behaviour, not the implementation.

```go
//counterfeiter:generate -o ../../mocks/pod_quota_limiter.go --fake-name FakePodQuotaLimiter . PodQuotaLimiter

// PodQuotaLimiter reports the pod capacity the namespace ResourceQuota enforces
// for one agent priority class.
type PodQuotaLimiter interface {
	// PodQuota returns the pod limit of the ResourceQuota whose scopeSelector
	// matches PriorityClass In [priorityClassName] in namespace. It returns 0
	// with a nil error when no quota bounds that priority class.
	PodQuota(
		ctx context.Context,
		namespace libk8s.Namespace,
		priorityClassName string,
	) (int, error)
}

// NewPodQuotaLimiter creates a PodQuotaLimiter backed by the K8s core/v1 API.
func NewPodQuotaLimiter(
	kubeClient kubernetes.Interface,
	currentDateTime libtime.CurrentDateTimeGetter,
) PodQuotaLimiter
```

Use imports `libk8s "github.com/bborbe/k8s"` and `libtime "github.com/bborbe/time"` in this file.

2. Implement the private `podQuotaLimiter` struct with fields `kubeClient kubernetes.Interface`, `currentDateTime libtime.CurrentDateTimeGetter`, a `sync.Mutex`, and a `map[string]cachedPodQuota` where `cachedPodQuota` holds `limit int` and `readAt time.Time`. Add `const podQuotaCacheTTL = 30 * time.Second` with a comment stating the cache bounds admission's API cost to one list per namespace and priority class per window.

3. Implement `PodQuota` in this order:
   - Return `(0, nil)` immediately when `priorityClassName == ""` — no API call, no cache entry. This is the "Config names no priority class" case.
   - Build the cache key as `namespace.String() + "/" + priorityClassName`. Read the clock once: `now := l.currentDateTime.Now().Time()`. Take the mutex only for the map read/write, never across the API call.
   - On a cache hit whose `now.Sub(cached.readAt) < podQuotaCacheTTL`, return `cached.limit, nil`.
   - Otherwise call `l.kubeClient.CoreV1().ResourceQuotas(namespace.String()).List(ctx, metav1.ListOptions{})`.
   - On a list error return `(0, errors.Wrapf(ctx, err, "list resourcequotas in namespace %s", namespace))` and do NOT write a cache entry — a failed read must be retried by the next admission, not cached for 30 s.
   - On success compute the limit with the helper in requirement 4, store `cachedPodQuota{limit: limit, readAt: now}`, and return `(limit, nil)`.

4. Add unexported helpers:
   - `quotaScopesPriorityClass(quota *corev1.ResourceQuota, priorityClassName string) bool` — false when `quota.Spec.ScopeSelector == nil`; otherwise true when any `MatchExpressions` entry has `ScopeName == corev1.ResourceQuotaScopePriorityClass`, `Operator == corev1.ScopeSelectorOpIn`, and `priorityClassName` in `Values`. Operators other than `In` (`Exists`, `NotIn`, `DoesNotExist`) do not match.
   - `quotaPodLimit(quota *corev1.ResourceQuota) (int, bool)` — return `status.Hard[corev1.ResourcePods]` when present, else `spec.Hard[corev1.ResourcePods]` when present (assumption: `status.hard` mirrors `spec.hard` after the quota controller processes the object, `spec.hard` is the fallback while it has not). Return `(0, false)` when neither carries `pods`.
   - `podQuotaFromQuotas(quotas []corev1.ResourceQuota, priorityClassName string) int` — iterate, skip quotas that do not scope the priority class, skip a matching quota whose `pods` limit is missing or `<= 0`, and keep the smallest positive limit. Collect the names of every quota that contributed a positive limit. When more than one contributed, log exactly one warning with `glog.Warningf` in this shape, naming every contributing quota and the limit that won:

```
event=pod_quota_ambiguous priority_class=%s quotas=%s using=%d
```

   Return `0` when nothing matched.

5. Create `pkg/spawner/pod_quota_limiter_test.go`, `package spawner_test`, Ginkgo style matching `job_spawner_test.go`. Add a local `captureSpawnerLog(fn func()) string` helper copied from `capturePinReportLog` in `pkg/handler/task_event_handler_pin_outside_pool_test.go` (Ginkgo `Expect` assertions, `flag.Set("logtostderr", "true")`, `os.Stderr` pipe, `glog.Flush()`, restore the flag). Build fixtures with local helpers that construct `*corev1.ResourceQuota` objects carrying a `ScopeSelector` and `Status.Hard{corev1.ResourcePods: resource.MustParse("<n>")}`. Cover these cases as table rows where the shape allows and individual `It`s where it does not:
   - a quota scoped `PriorityClass In ["github-pr-review-agent"]` with `pods: 3` returns 3;
   - a namespace holding only a quota scoped to a different priority class returns 0;
   - a namespace holding that unrelated quota together with a matching `pods: 3` quota returns 3 (the unrelated quota is ignored, not treated as "no match");
   - a quota with a nil `ScopeSelector` returns 0;
   - two matching quotas with `pods: 5` and `pods: 3` return 3, and `captureSpawnerLog` output contains exactly one `event=pod_quota_ambiguous` line whose text names both quota object names and `using=3` (do not assert the order the two names appear in — the fake client's list order is not part of the contract);
   - no quota at all returns 0 with a nil error;
   - an empty priority class returns 0 with a nil error and `len(fakeClient.Actions()) == 0` (no API call);
   - a quota whose `Status.Hard` is empty and whose `Spec.Hard` carries `pods: 2` returns 2;
   - a quota built from the **literal strings** `ScopeName: "PriorityClass"`, `Operator: "In"` (not the `corev1` constants) with `pods: 4` returns 4 — this pins the constant values themselves, so an implementation that compared against the wrong library constant fails on one side only;
   - a quota scoped `PriorityClass NotIn ["github-pr-review-agent"]` returns 0 (only the `In` operator matches).
6. Add a caching spec to the same file: one matching quota, `currentDateTime.SetNow(t0)`; call `PodQuota` at `t0`, at `t0+10s`, and at `t0+31s`; every call returns 3; count the list actions with a helper that filters `fakeClient.Actions()` on `action.Matches("list", "resourcequotas")` and assert exactly 2 across the three calls.
7. Add an error spec: prepend a reactor that counts calls and fails only the FIRST list with `(true, nil, stderrors.New("forbidden"))` (subsequent calls fall through with `(false, nil, nil)` to the object tracker). Assert the first `PodQuota` returns a non-nil error, the second `PodQuota` returns 3 with a nil error, and the list counter reads 2 — the failed read was not cached.
8. Generate the fake for the new interface from the repository root:

```
go generate -mod=mod ./pkg/spawner/...
```

   Confirm `mocks/pod_quota_limiter.go` exists and declares `FakePodQuotaLimiter` with `PodQuota`, `PodQuotaReturns`, `PodQuotaCallCount` and `PodQuotaArgsForCall`. The next prompt consumes it. Do not edit `mocks/job_spawner.go` or any other generated file by hand.

9. Before finishing, re-run `<verification>` and walk each acceptance criterion of this prompt against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change any existing file in `pkg/spawner` except adding the new file and its test; `job_spawner.go` and the `JobSpawner` interface stay byte-identical.
- Do NOT wire the limiter into the handler or the factory in this prompt — nothing may call `PodQuotaLimiter` yet.
- Do NOT add a Prometheus metric. The only observability here is the one ambiguity warning.
- Do NOT add a `ResourceQuota` manifest, RBAC rule, or Helm change — the read grant is a separate change in the `bborbe/agent` chart and a denied read is handled as an ordinary error.
- Do NOT cache a failed read. The failure mode in the spec is "the next admission re-reads".
- Do NOT change `LeaseDuration`, `RenewInterval`, `FenceMargin`, `TakeoverGrace`, `JoblessGrace`, `JoblessCheckInterval`, claim keys, takeover, or per-PR keying — none are touched by this prompt.
- Keys, task ids and priority class names are never Prometheus label values.
- Existing tests must still pass unchanged.
- Error wrapping uses `github.com/bborbe/errors` (`errors.Wrapf(ctx, err, ...)`), never `fmt.Errorf`, and never `context.Background()` inside `pkg/`.
- File length: keep each new Go file under 400 lines; split helpers into a second file in the same package if needed.
</constraints>

<verification>
```
ROOTDIR=/workspace make test
ROOTDIR=/workspace make precommit
grep -n "PodQuotaLimiter" mocks/pod_quota_limiter.go
```
`make test` and `make precommit` must exit 0. The grep must print the generated fake's type/constructor lines.
</verification>
