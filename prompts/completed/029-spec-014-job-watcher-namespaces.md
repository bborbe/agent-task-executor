---
status: completed
spec: [014-job-watcher-sandbox-namespaces]
summary: JobWatcher now takes the Config provider, derives a per-namespace watched set, runs one Job/Pod informer pair per namespace with a per-namespace cache-sync gate, and fails Run only on wiring errors
execution_id: agent-task-executor-job-watcher-sandbox-exec-029-spec-014-job-watcher-namespaces
dark-factory-version: v0.198.7
created: "2026-10-23T18:45:00Z"
queued: "2026-10-23T17:26:27Z"
started: "2026-10-23T17:26:29Z"
completed: "2026-10-23T17:38:36Z"
branch: dark-factory/job-watcher-sandbox-namespaces
---

<summary>
- The Job watcher now receives the Config provider, so it can learn which namespaces its Configs place Jobs in.
- The watched namespace set is derived, not hand-listed: the executor's own namespace plus every distinct namespace a Config resolves to.
- A Config whose `jobNamespace` equals the executor namespace is skipped with an ERROR naming the Config; the watcher keeps running for every other namespace.
- One informer pair (Jobs and Pods) is built and started per watched namespace, each with the existing 5-minute resync and task-id label selector.
- The cache-sync barrier is per namespace: an event is handled only when its own namespace has synced; an unsynced or unconfigured namespace is ignored at the handler.
- A wiring failure (provider at startup, informer construction, handler registration) fails the run loudly; an environmental sync failure does not.
- Existing direct-call handler tests keep working because the executor namespace starts as watched and synced until `Run` starts its informer.
- The stale-event guard that asks whether a task still has a running Job now asks in the Job's own namespace instead of always the executor's.
- No new RBAC, no cluster-scoped informer, no change to spawning or namespace resolution.
</summary>

<objective>
Give `JobWatcher` the `libk8s.Provider[agentv1.Config]`, derive the watched namespace set from the executor namespace plus every Config's resolved `jobNamespace`, build and start one Job/Pod informer pair per namespace, gate `HandleJob`/`HandlePod` on that namespace being watched and synced, and fail the run on wiring errors while treating list/watch failures during sync as environmental. End state: a terminal-failed Job in a sandbox namespace is handled exactly like one in the executor namespace, and an unconfigured namespace is never handled.
</objective>

<context>
Read these files fully before editing:

- `pkg/job_watcher.go` — the whole file. Key surfaces:
  - `type JobWatcher interface` with `Run`, `HandleJob`, `HandlePod`, `PodLister`.
  - `func NewJobWatcher(kubeClient kubernetes.Interface, namespace libk8s.Namespace, taskStore *TaskStore, publisher ResultPublisher, claimReleaser ClaimReleaser) JobWatcher` and the `jobWatcher` struct.
  - `func (w *jobWatcher) Run(ctx context.Context) error` — currently builds one factory scoped to `w.namespace`, adds handlers, calls `factory.Start(ctx.Done())`, `cache.WaitForCacheSync`, stores the lister, then blocks.
  - `func (w *jobWatcher) PodLister() corev1listers.PodLister`.
  - `func (w *jobWatcher) HandleJob` and `HandlePod` — the entry points the gate goes into.
  - `func (w *jobWatcher) taskHasActiveJob(ctx context.Context, taskID lib.TaskIdentifier) (bool, error)` — currently lists in `w.namespace`.
- `pkg/job_namespace.go` — `func ResolveJobNamespace(ctx context.Context, executorNamespace libk8s.Namespace, jobNamespace string) (libk8s.Namespace, error)`. Empty means the executor namespace; a value equal to the executor namespace returns an error. Reuse it; do not reimplement the rule.
- `pkg/event_handler_config.go` — `type EventHandlerConfig k8s.EventHandler[agentv1.Config]`, and `func NewEventHandlerConfig() EventHandlerConfig`. `EventHandler[T]` embeds `Provider[T]` (`Get(ctx) ([]T, error)`), so it satisfies `libk8s.Provider[agentv1.Config]`.
- `pkg/factory/factory.go` — `func CreateJobWatcher` (the only factory for this type) and `func CreateServiceReconcileLoop(provider libk8s.Provider[agentv1.Config], ...)` for the existing provider parameter style.
- `main.go` around lines 241-360 — `eventHandlerConfig := factory.CreateEventHandlerConfig()` is already passed to `CreateServiceReconcileLoop` and `CreateZombieSweeper`; the same value goes to `CreateJobWatcher`.
- Every existing `NewJobWatcher` call site must be updated in this prompt:
  - `pkg/job_watcher_test.go` (package `pkg_test`)
  - `pkg/job_watcher_internal_test.go` (package `pkg`)
  - `pkg/handler/task_event_handler_claim_test.go` (package `handler_test`)
  - `pkg/handler/task_event_handler_respawn_race_test.go` (package `handler_test`)
  - `pkg/envtest/job_watcher_envtest_test.go` (package `envtest`)
- Test conventions:
  - `pkg/suite_test.go` holds the Ginkgo entry point `TestPkg`; plain `func TestXxx(t *testing.T)` functions in `pkg` run alongside it and are selected by `-run`.
  - `pkg/job_watcher_internal_test.go` is `package pkg` and defines `fakeResultPublisher` with `PublishFailureCallCount()` / `PublishFailureArgsForCall(i)` — reuse it from the new test file.
  - `pkg/job_watcher_internal_test.go` also defines `setPodLister`, `newTestPodLister`, and `errorPodLister`. Leave those alone in this prompt; prompt 3 rewrites them.

Guidelines (read before writing Go):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-precommit.md`
</context>

<requirements>
1. In `pkg/job_watcher.go`, add the Config provider and the per-namespace bookkeeping to `jobWatcher`.
   - Add import `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"` and `"sort"`, `"sync"`.
   - Add these fields to `type jobWatcher struct` (keep `podLister atomic.Pointer[corev1listers.PodLister]` — prompt 3 removes it):
     ```go
     configProvider       libk8s.Provider[agentv1.Config]
     mu                   sync.Mutex
     watched              map[libk8s.Namespace]bool
     synced               map[libk8s.Namespace]bool
     informers            map[libk8s.Namespace]jobWatcherNamespaceInformer
     newNamespaceInformer func(kubernetes.Interface, libk8s.Namespace) (jobWatcherNamespaceInformer, error)
     ```
   - Change the constructor signature to:
     ```go
     func NewJobWatcher(
         kubeClient kubernetes.Interface,
         namespace libk8s.Namespace,
         configProvider libk8s.Provider[agentv1.Config],
         taskStore *TaskStore,
         publisher ResultPublisher,
         claimReleaser ClaimReleaser,
     ) JobWatcher
     ```
     The literal must use the substring `Provider[agentv1.Config]` (an acceptance check greps for it).
   - In the constructor, initialise the maps, set `newNamespaceInformer: newJobWatcherNamespaceInformer`, and seed the executor namespace:
     ```go
     w := &jobWatcher{...}
     w.watched = map[libk8s.Namespace]bool{namespace: true}
     w.synced = map[libk8s.Namespace]bool{namespace: true}
     w.informers = map[libk8s.Namespace]jobWatcherNamespaceInformer{}
     ```
     The seed is the pre-`Run` contract for the existing direct-call tests: before any informer is started, a handler call for the executor namespace is trusted. `startNamespaceInformer` clears the synced bit when it starts that namespace's informer.

2. Add the per-namespace informer seam in `pkg/job_watcher.go`:
   ```go
   // jobWatcherNamespaceInformer is one namespace's Job and Pod informers. It is
   // an interface so the namespace-set tests can count starts and control the
   // cache-sync barrier without an API server.
   type jobWatcherNamespaceInformer interface {
       Start(ctx context.Context)
       HasSynced() bool
       AddJobEventHandler(handler cache.ResourceEventHandler) error
       AddPodEventHandler(handler cache.ResourceEventHandler) error
       PodLister() corev1listers.PodLister
   }
   ```
   Add the production implementation backed by client-go:
   ```go
   const (
       jobWatcherResyncPeriod = 5 * time.Minute
       jobWatcherTaskIDLabel  = "agent.benjamin-borbe.de/task-id"
   )

   type sharedInformerNamespaceInformer struct {
       factory k8sinformers.SharedInformerFactory
       job     cache.SharedIndexInformer
       pod     cache.SharedIndexInformer
   }
   ```
   - `Start(ctx)` calls `s.factory.Start(ctx.Done())`.
   - `HasSynced()` returns `s.job.HasSynced() && s.pod.HasSynced()`.
   - `AddJobEventHandler` / `AddPodEventHandler` call `s.job.AddEventHandler` / `s.pod.AddEventHandler` and return the error.
   - `PodLister()` returns `s.factory.Core().V1().Pods().Lister()`.
   - Add the package-level constructor (the production value of the field):
     ```go
     var newJobWatcherNamespaceInformer = func(
         kubeClient kubernetes.Interface,
         namespace libk8s.Namespace,
     ) (jobWatcherNamespaceInformer, error) {
         factory := k8sinformers.NewSharedInformerFactoryWithOptions(
             kubeClient,
             jobWatcherResyncPeriod,
             k8sinformers.WithNamespace(namespace.String()),
             k8sinformers.WithTweakListOptions(func(opts *metav1.ListOptions) {
                 opts.LabelSelector = jobWatcherTaskIDLabel
             }),
         )
         return &sharedInformerNamespaceInformer{
             factory: factory,
             job:     factory.Batch().V1().Jobs().Informer(),
             pod:     factory.Core().V1().Pods().Informer(),
         }, nil
     }
     ```

3. Add the pure namespace derivation:
   ```go
   // resolveJobWatcherNamespaces returns the namespaces the watcher must observe:
   // the executor's own namespace unconditionally, plus the resolved jobNamespace
   // of every Config. Duplicates collapse. A Config whose jobNamespace fails
   // resolution is skipped with an ERROR naming the Config and the rejected value;
   // it spawns no Jobs in that state, and one bad Config must not stop the watcher
   // for every other namespace.
   func resolveJobWatcherNamespaces(
       ctx context.Context,
       executorNamespace libk8s.Namespace,
       cfgs []agentv1.Config,
   ) []libk8s.Namespace
   ```
   Implementation: seed a `map[libk8s.Namespace]struct{}{executorNamespace: {}}`; for each Config call `ResolveJobNamespace(ctx, executorNamespace, cfg.Spec.JobNamespace)`; on error `glog.Errorf("job watcher: skip config %s: %v", cfg.Name, err)` and continue; otherwise add the result. Return the keys as a `[]libk8s.Namespace` sorted with `sort.Slice` so the order is deterministic.

4. Replace the body of `Run` with a one-time resolution followed by a block. Prompt 2 adds the re-resolution ticker; do not add it here.
   ```go
   func (w *jobWatcher) Run(ctx context.Context) error {
       if err := w.reconcileNamespaceSet(ctx); err != nil {
           return err
       }
       <-ctx.Done()
       return nil
   }
   ```

5. Add the resolution/start helpers:
   ```go
   func (w *jobWatcher) reconcileNamespaceSet(ctx context.Context) error {
       cfgs, err := w.configProvider.Get(ctx)
       if err != nil {
           return errors.Wrapf(ctx, err, "list configs for job watcher namespaces")
       }
       return w.startInformersForNamespaces(ctx, resolveJobWatcherNamespaces(ctx, w.namespace, cfgs))
   }

   func (w *jobWatcher) startInformersForNamespaces(
       ctx context.Context,
       namespaces []libk8s.Namespace,
   ) error {
       for _, namespace := range namespaces {
           if w.informerStarted(namespace) {
               continue
           }
           if err := w.startNamespaceInformer(ctx, namespace); err != nil {
               return err
           }
       }
       return nil
   }

   func (w *jobWatcher) informerStarted(namespace libk8s.Namespace) bool
   ```
   `informerStarted` takes `w.mu`, reports whether `w.informers[namespace]` exists, and releases the lock.

6. Implement `startNamespaceInformer`:
   ```go
   func (w *jobWatcher) startNamespaceInformer(
       ctx context.Context,
       namespace libk8s.Namespace,
   ) error
   ```
   Steps, in order:
   a. `informer, err := w.newNamespaceInformer(w.kubeClient, namespace)`; on error return `errors.Wrapf(ctx, err, "create informers for namespace %s", namespace)`.
   b. Build `cache.ResourceEventHandlerFuncs` for Jobs (`AddFunc` and `UpdateFunc` cast `*batchv1.Job` and call `w.HandleJob(ctx, job)`) and register with `informer.AddJobEventHandler`; on error return `errors.Wrapf(ctx, err, "add job event handler for namespace %s", namespace)`.
   c. Build the same for Pods (`*corev1.Pod`, `w.HandlePod`) and register with `informer.AddPodEventHandler`; on error return `errors.Wrapf(ctx, err, "add pod event handler for namespace %s", namespace)`.
   d. Under `w.mu`: set `w.watched[namespace] = true`, `w.synced[namespace] = false`, and store `w.informers[namespace] = informer`.
   e. Call `informer.Start(ctx)`.
   f. `glog.V(2).Infof("job and pod informers started in namespace %s", namespace)`.
   Storing the informer before `Start` is deliberate: events delivered during the initial list must find the informer entry and be rejected by the unsynced gate.

7. Add the gate and use it from both handlers:
   ```go
   // namespaceReady reports whether the watcher may handle events for namespace.
   // A namespace no Config names is never ready. Before an informer is started
   // for a watched namespace, the pre-Run seed decides; once it is started, the
   // namespace's own cache-sync state decides.
   func (w *jobWatcher) namespaceReady(namespace libk8s.Namespace) bool
   ```
   Implementation under `w.mu`:
   - `if !w.watched[namespace] { return false }`
   - `informer, ok := w.informers[namespace]`; `if !ok { return w.synced[namespace] }`
   - `if w.synced[namespace] { return true }`
   - `if informer.HasSynced() { w.synced[namespace] = true; return true }`
   - `return false`
   Add `if !w.namespaceReady(libk8s.Namespace(job.Namespace)) { return }` as the first statement of `HandleJob`, and `if !w.namespaceReady(libk8s.Namespace(pod.Namespace)) { return }` as the first statement of `HandlePod`.

8. Make the active-Job lookup namespace-aware. Change the signature to
   ```go
   func (w *jobWatcher) taskHasActiveJob(
       ctx context.Context,
       namespace string,
       taskID lib.TaskIdentifier,
   ) (bool, error)
   ```
   and list with `w.kubeClient.BatchV1().Jobs(namespace).List(...)`. Update both callers in `handleSucceeded` and `publishSyntheticFailure` to pass `job.Namespace`. A sandbox Job's stale-re-delivery guard must ask about the Job's own namespace, not the executor's.

9. Keep `PodLister()` compiling and behaviourally correct for the executor namespace until prompt 3 changes its signature:
   ```go
   func (w *jobWatcher) PodLister() corev1listers.PodLister {
       if lister := w.podLister.Load(); lister != nil {
           return *lister
       }
       return w.podListerForNamespace(w.namespace)
   }
   ```
   Add the helper (prompt 3 keeps it and makes it the public method):
   ```go
   func (w *jobWatcher) podListerForNamespace(
       namespace libk8s.Namespace,
   ) corev1listers.PodLister
   ```
   Under `w.mu`: return `nil` when no informer exists for the namespace; when `w.synced[namespace]` is false, return `nil` unless `informer.HasSynced()` is true (then set the bit); otherwise return `informer.PodLister()`.

10. Update `pkg/factory/factory.go`:
    ```go
    func CreateJobWatcher(
        kubeClient kubernetes.Interface,
        namespace libk8s.Namespace,
        configProvider libk8s.Provider[agentv1.Config],
        taskStore *pkg.TaskStore,
        publisher pkg.ResultPublisher,
        claimReleaser pkg.ClaimReleaser,
    ) pkg.JobWatcher {
        return pkg.NewJobWatcher(kubeClient, namespace, configProvider, taskStore, publisher, claimReleaser)
    }
    ```
    Update the doc comment to say it observes every Config's namespace.

11. In `main.go`, pass `eventHandlerConfig` to `CreateJobWatcher`:
    ```go
    jobWatcher := factory.CreateJobWatcher(
        kubeClient,
        a.Namespace,
        eventHandlerConfig,
        taskStore,
        resultPublisher,
        taskEventHandler,
    )
    ```

12. Update every existing `NewJobWatcher` call site to insert an empty provider immediately after the namespace argument:
    - `pkg/job_watcher_test.go`: `pkg.NewJobWatcher(fakeKubeClient, "test-ns", pkg.NewEventHandlerConfig(), taskStore, fakePublisher, nil)`
    - `pkg/job_watcher_internal_test.go`: `NewJobWatcher(fake.NewSimpleClientset(), "test-ns", NewEventHandlerConfig(), taskStore, fakePublisher, nil)`
    - `pkg/handler/task_event_handler_claim_test.go`: insert `pkg.NewEventHandlerConfig()` after `"test-ns"`.
    - `pkg/handler/task_event_handler_respawn_race_test.go`: insert `pkg.NewEventHandlerConfig()` after `"test-ns"`.
    - `pkg/envtest/job_watcher_envtest_test.go`: `pkg.NewJobWatcher(kubeClient, libk8s.Namespace(ns), pkg.NewEventHandlerConfig(), store, publisher, nil)`
    These tests use the executor namespace, which the constructor seeds as watched and synced, so their direct `HandleJob`/`HandlePod` calls keep passing without further changes.

13. Create `pkg/job_watcher_namespaces_test.go`, `package pkg`, with the repository's BSD license header, and add the fakes plus the six tests below. Do not use Ginkgo; use plain `func TestXxx(t *testing.T)` so `go test -run <Name>` selects them. Every test must contain at least two `t.Fatalf(...)` calls on separate lines.
    Fakes/helpers in this file (prompt 2 and prompt 3 append tests that reuse them):
    ```go
    type namespaceTestProvider struct {
        mu    sync.Mutex
        items []agentv1.Config
        err   error
    }
    func (p *namespaceTestProvider) Get(ctx context.Context) ([]agentv1.Config, error)
    func (p *namespaceTestProvider) setItems(items ...agentv1.Config)
    func (p *namespaceTestProvider) setErr(err error)

    type fakeNamespaceInformer struct {
        mu        sync.Mutex
        synced    bool
        started   int
        podLister corev1listers.PodLister
        addJobErr error
        addPodErr error
    }
    func (f *fakeNamespaceInformer) Start(ctx context.Context)
    func (f *fakeNamespaceInformer) HasSynced() bool
    func (f *fakeNamespaceInformer) AddJobEventHandler(handler cache.ResourceEventHandler) error
    func (f *fakeNamespaceInformer) AddPodEventHandler(handler cache.ResourceEventHandler) error
    func (f *fakeNamespaceInformer) PodLister() corev1listers.PodLister

    // failedJob builds a terminal-failed Job named name, in namespace, carrying
    // taskID under the agent.benjamin-borbe.de/task-id label and a true
    // batchv1.JobFailed condition.
    func failedJob(name, taskID, namespace string) *batchv1.Job
    ```
    Do not declare a `namespacePodLister` helper here — none of this prompt's tests call it, and `.golangci.yml` enables `unused` with `run.tests: true`, so an unexported uncalled test helper fails `make precommit`. It is added by prompt 3, whose tests are the first to call it.
    `namespaceTestProvider.Get` copies the slice under the mutex and returns the configured error first. Every test that configures Configs MUST do it through `setItems(...)`, never a struct literal — an unexported method no test calls is rejected by the same `unused` linter.
    `failedJob` sets `ObjectMeta{Name, Namespace, Labels: map[string]string{"agent.benjamin-borbe.de/task-id": taskID}}` and `Status.Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}`.
    - `func TestJobWatcherNamespaces(t *testing.T)`: call `resolveJobWatcherNamespaces(context.Background(), "dev", cfgs)` for four fixtures and compare the sorted `[]string` form:
      (a) Configs with `JobNamespace` values `""`, `"dev-agents-sandbox"`, `"dev-agents-sandbox"`, `"dev-other"` → exactly `[dev dev-agents-sandbox dev-other]`;
      (b) the same Configs without the empty one → still `[dev dev-agents-sandbox dev-other]` (the executor namespace is unconditional, not only via the empty rule);
      (c) no Configs → `[dev]`;
      (d) one Config with `JobNamespace: "dev"` → `[dev]` (skipped; the function returns a slice, so there is no error to assert).
      Use `t.Fatalf` for each mismatch.
    - `func TestJobWatcherTerminalFailureAcrossNamespaces(t *testing.T)`: provider with two Configs, one `JobNamespace: ""` and one `"dev-agents-sandbox"`. Build the watcher with the checked assertion form the repo already uses (`pkg/job_watcher_internal_test.go:214`) — `forcetypeassert` is enabled with no `_test.go` exclusion, so a bare assertion fails `make precommit`: `w, ok := NewJobWatcher(fake.NewClientset(), "dev", provider, taskStore, publisher, nil).(*jobWatcher)`, `t.Fatalf` when `!ok`; set `w.newNamespaceInformer` to a function returning a `*fakeNamespaceInformer{synced: true}` for every namespace; call `w.reconcileNamespaceSet(ctx)` and `t.Fatalf` on error. Store a task, then `w.HandleJob(ctx, failedJob("job-sandbox", string(taskID), "dev-agents-sandbox"))`. Assert `publisher.PublishFailureCallCount() == 1`, the recorded job name equals `"job-sandbox"`, and `taskStore.Load(taskID)` reports absent. Use `fakeResultPublisher` from `job_watcher_internal_test.go`.
    - `func TestJobWatcherThreeNamespaces(t *testing.T)`: provider with Configs naming `ns-a`, `ns-b`, `ns-c`; reconcile with synced fake informers; deliver one failed Job in each with distinct task IDs; assert three publishes and all three store entries gone. A two-namespace implementation must fail this test.
    - `func TestJobWatcherIgnoresUnconfiguredNamespace(t *testing.T)`: provider names `ns-a` only; reconcile; store a task; deliver a failed Job in `"dev-other"`; assert `PublishFailureCallCount() == 0` and the store entry is still present.
    - `func TestJobWatcherFailsLoudly(t *testing.T)`: two subtests.
      (a) `provider.setErr(stderrors.New("config provider down"))`; `err := w.Run(context.Background())`; `t.Fatalf` if `err == nil`.
      (b) provider names `ns-a`; set `w.newNamespaceInformer = func(kubernetes.Interface, libk8s.Namespace) (jobWatcherNamespaceInformer, error) { return nil, stderrors.New("informer construction failed") }`; `err := w.Run(context.Background())`; `t.Fatalf` if `err == nil`.
      (c) provider names `ns-a`; `w.newNamespaceInformer` returns `&fakeNamespaceInformer{synced: true, addJobErr: stderrors.New("job handler registration failed")}`; `err := w.Run(ctx)`; `t.Fatalf` if `err == nil`.
      (d) same as (c) with `addPodErr: stderrors.New("pod handler registration failed")`; `t.Fatalf` if `err == nil`.
      Use `ctx, cancel := context.WithTimeout(context.Background(), time.Second)` and `defer cancel()` in each subtest, so an implementation that ignores the error fails instead of hanging the suite.
      Do not test list/watch failure during sync here; that is environmental and must not fail the run.
    - `func TestJobWatcherAwaitsCacheSync(t *testing.T)`: provider names `ns-a` and `ns-b`; `w.newNamespaceInformer` returns a synced fake for `dev` and `ns-b` and an unsynced fake (`synced: false`) for `ns-a`; reconcile. Store a task; deliver a failed Job in `ns-a` → assert `PublishFailureCallCount() == 0` and the store entry remains. Store a second task; deliver a failed Job in `ns-b` → assert exactly one publish. The two assertions prove the barrier is per namespace, not global.

14. Every new file starts with:
    ```go
    // Copyright (c) 2026 Benjamin Borbe All rights reserved.
    // Use of this source code is governed by a BSD-style
    // license that can be found in the LICENSE file.
    ```

15. Before finishing, re-run `<verification>` and confirm every command passes, then walk AC2, AC3, AC4, AC5, AC6, AC7, AC9 and AC11 of the spec against the change and state for each whether it is met.
</requirements>

<constraints>
- No new RBAC of any kind: no Role, ClusterRole, RoleBinding, ClusterRoleBinding, ServiceAccount or manifest. The existing `agent-task-executor-dev` ClusterRoleBinding is not leaned on.
- Do not add a cluster-scoped informer. Every informer is created with `k8sinformers.WithNamespace(...)`.
- The executor's own namespace stays watched unconditionally; behaviour for every Config without `JobNamespace` must be byte-identical, with one accepted exception: an event delivered while its namespace is still listing is dropped and re-delivered at the informer's next 5-minute resync rather than at the initial LIST. Do not "fix" that — it is the per-namespace sync barrier, and the spec's "initial LIST replays" wording is superseded by it.
- Do not change `ResolveJobNamespace`, the CRD types, the spawner, or the Kafka publishing path.
- A wiring failure at startup (provider list, informer construction, handler registration) fails `Run`; a list/watch failure during cache sync must not (it is retried by client-go). Do not add custom retry logic for sync.
- Existing tests must still pass; do not weaken any existing assertion. The handler gate is a new precondition on every direct `HandleJob`/`HandlePod` call; the constructor seed covers the executor namespace.
- Linter limits from `go-precommit.md`: `funlen` 80 lines per function, `nestif` 4, `golines` max line 100. Split test helpers rather than growing one function past 80 lines.
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
- The Post-Deploy acceptance check and the no-RBAC diff guard are host-side checks; this prompt adds no RBAC and no manifest.
</constraints>

<verification>
Run from the repository root:

```bash
GOFLAGS=-buildvcs=false go test ./... -run 'TestJobWatcherNamespaces|TestJobWatcherTerminalFailureAcrossNamespaces|TestJobWatcherThreeNamespaces|TestJobWatcherIgnoresUnconfiguredNamespace|TestJobWatcherFailsLoudly|TestJobWatcherAwaitsCacheSync' -v
```

The stdout must contain `--- PASS` for each of those six test names.

Then:

```bash
awk '/^func CreateJobWatcher/,/^}/' pkg/factory/factory.go | grep -c 'Provider\[agentv1.Config\]'
grep -c 'Provider\[agentv1.Config\]' pkg/job_watcher.go
awk '/^func Test/{t=$2; sub(/\(.*/,"",t)} /(Fatalf|Errorf|require\.|assert\.|Expect\()/{c[t]++} END{for(k in c) print k, c[k]}' pkg/job_watcher_namespaces_test.go
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

The first two greps must print a number ≥ 1. The `awk` must list each of the six test names with a count ≥ 2. `make precommit` must exit 0.
</verification>
