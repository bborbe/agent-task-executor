---
status: approved
spec: [014-job-watcher-sandbox-namespaces]
created: "2026-10-23T18:45:00Z"
queued: "2026-10-23T17:26:27Z"
branch: dark-factory/job-watcher-sandbox-namespaces
---

<summary>
- The watcher re-derives its namespace set every 60 seconds, so a Config that starts naming a new sandbox namespace is observed without an executor restart.
- A namespace already being watched is left alone; only a namespace not yet watched gets a new informer pair.
- A provider failure on the tick is logged and skipped, not fatal; a wiring failure while starting a new namespace still fails the run.
- A namespace removed from the Config set keeps its informer until process exit, by design.
- The tick interval is a package-level variable, not configuration and not an injected clock, so the test can shorten it.
- The tick and the event handlers take the same lock, so a handler never observes a half-updated namespace set.
- No new RBAC and no change to how a namespace is resolved or how a Job is spawned.
</summary>

<objective>
Add the 60-second re-resolution loop to `JobWatcher.Run`: after the startup resolution, tick on a package-level interval, re-list the Configs, and start informers only for namespaces not already watched. End state: a Config flip that names a new `jobNamespace` is picked up within one interval with no restart, while an already-watched namespace is not restarted.
</objective>

<context>
Read these files fully before editing:

- `pkg/job_watcher.go` — especially the prompt-1 additions: `reconcileNamespaceSet`, `startInformersForNamespaces`, `startNamespaceInformer`, `resolveJobWatcherNamespaces`, the `jobWatcher` struct's `mu`/`watched`/`synced`/`informers`/`newNamespaceInformer` fields, and `Run`.
- `pkg/job_watcher_namespaces_test.go` — prompt 1 created it (`package pkg`) with `namespaceTestProvider`, `fakeNamespaceInformer`, `namespacePodLister`, and the six namespace tests. Append to this file; reuse those fakes.
- `pkg/job_namespace.go` — `ResolveJobNamespace` (already used by the derivation).
- `pkg/factory/factory.go` and `main.go` — no changes needed here; the interval is not configuration.
- `docs/agent-crd-specification.md` — the `spec.jobNamespace` semantics are unchanged; this prompt adds no documentation.

Guidelines:
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-concurrency-patterns.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-context-cancellation-in-loops.md`
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md`
</context>

<requirements>
1. In `pkg/job_watcher.go`, add the package-level interval variable with this exact default:
   ```go
   // jobWatcherNamespaceReresolveInterval is how often the watched namespace set
   // is re-derived from the Configs. It is a package-level variable, not
   // configuration and not an injected clock, so tests can shorten it directly.
   var jobWatcherNamespaceReresolveInterval = 60 * time.Second
   ```
2. Replace the blocking tail of `Run` with a ticker loop. Keep the startup resolution fatal and keep the provider-failure-on-tick non-fatal:
   ```go
   func (w *jobWatcher) Run(ctx context.Context) error {
       if err := w.reconcileNamespaceSet(ctx); err != nil {
           return err
       }
       ticker := time.NewTicker(jobWatcherNamespaceReresolveInterval)
       defer ticker.Stop()
       for {
           select {
           case <-ctx.Done():
               return nil
           case <-ticker.C:
               cfgs, err := w.configProvider.Get(ctx)
               if err != nil {
                   // The Config provider is populated asynchronously and can fail
                   // transiently; the already-running informers keep watching and
                   // the next tick retries. Killing the executor over this is the
                   // worse outcome.
                   glog.Errorf("job watcher: re-resolve namespace set: %v", err)
                   continue
               }
               if err := w.startInformersForNamespaces(
                   ctx,
                   resolveJobWatcherNamespaces(ctx, w.namespace, cfgs),
               ); err != nil {
                   // A wiring failure while starting a new namespace's informers
                   // is deterministic and must fail the run, exactly as it does at
                   // startup.
                   return errors.Wrapf(ctx, err, "start informers for re-resolved namespaces")
               }
           }
       }
   }
   ```
   Do not call `reconcileNamespaceSet` from the tick: it wraps the provider error together with the wiring error, and the tick must distinguish them.
3. Do not stop or remove an informer for a namespace that disappears from the Config set. The `informers` map only grows until process exit; `startInformersForNamespaces` already skips a namespace whose informer exists.
4. Confirm the lock discipline: `startInformersForNamespaces` calls `informerStarted` (takes `w.mu`) and `startNamespaceInformer` (takes `w.mu` when inserting); `namespaceReady` and `podListerForNamespace` also take `w.mu`. Do not hold `w.mu` across `informer.Start` or `AddEventHandler`. If the prompt-1 code holds the lock across those calls, fix it in this prompt.
5. Append `func TestJobWatcherReresolvesNamespaceSet(t *testing.T)` to `pkg/job_watcher_namespaces_test.go` (`package pkg`). It must:
   - Build `provider := &namespaceTestProvider{}` with one Config naming `ns-one` (and `JobNamespace: "ns-one"`).
   - Define a small recording registry in this file (a type with a mutex, `informers map[libk8s.Namespace]*fakeNamespaceInformer`, and `constructions map[libk8s.Namespace]int`) whose `new(client kubernetes.Interface, namespace libk8s.Namespace) (jobWatcherNamespaceInformer, error)` increments the per-namespace construction count and returns a synced `*fakeNamespaceInformer`.
   - Build the watcher with the checked assertion form the repo already uses (`pkg/job_watcher_internal_test.go:214`) — `forcetypeassert` is enabled in `.golangci.yml` with no `_test.go` exclusion, so a bare assertion fails `make precommit`:
     ```go
     w, ok := NewJobWatcher(fake.NewClientset(), "dev", provider, taskStore, publisher, nil).(*jobWatcher)
     if !ok {
         t.Fatalf("NewJobWatcher did not return *jobWatcher")
     }
     w.newNamespaceInformer = registry.new
     ```
   - Override the interval and restore it with `t.Cleanup`:
     ```go
     old := jobWatcherNamespaceReresolveInterval
     jobWatcherNamespaceReresolveInterval = 10 * time.Millisecond
     t.Cleanup(func() { jobWatcherNamespaceReresolveInterval = old })
     ```
   - Start `Run` in a goroutine with a cancellable context and a buffered error channel.
   - Wait (bounded, e.g. 2 seconds, polling every 5 ms with `time.Sleep`) until the registry has constructed `dev` and `ns-one`. `t.Fatalf` on timeout.
   - Prove a provider failure on a tick is skipped, not fatal: call `provider.setErr(stderrors.New("config provider down"))` (import the standard library as `stderrors`, the alias `pkg/config_resolver.go` already uses — `github.com/bborbe/errors` is imported as `errors` in this package and has no `New`), sleep for at least three tick intervals, then assert with a non-blocking `select { case err := <-errCh: t.Fatalf("Run returned on a tick provider error: %v", err); default: }` that `Run` is still running, and call `provider.setErr(nil)`.
   - Add a second Config naming `ns-two` (`provider.setItems(...)`).
   - Wait until the registry has constructed `ns-two`. `t.Fatalf` on timeout.
   - Assert the new namespace was constructed exactly once: `if got := registry.count("ns-two"); got != 1 { t.Fatalf(...) }`.
   - Assert the already-watched namespace was not restarted: `if got := registry.count("ns-one"); got != 1 { t.Fatalf(...) }`.
   - Cancel the context, wait for `Run` to return (bounded), and `t.Fatalf` if it returned a non-nil error or did not return.
   Do not use a 60-second sleep and do not inject a clock dependency.
6. Every new code path in this prompt is covered by that one test; do not add a second near-duplicate test.
7. Before finishing, re-run `<verification>` and confirm every command passes, then walk AC8 and AC11 of the spec against the change and state for each whether it is met.
</requirements>

<constraints>
- The interval is a package-level `var` at 60 seconds in production. Do not add a Config field, CLI flag, environment variable, or clock interface for it.
- A provider failure on a tick is logged at ERROR and skipped; a wiring failure while starting informers is returned from `Run`. Do not collapse the two.
- Never stop or remove a running informer; a namespace removed from the Config set stays watched until process exit.
- Do not add RBAC, a cluster-scoped informer, or a namespace-list manifest.
- Existing tests must still pass; do not weaken any existing assertion.
- Linter limits: `funlen` 80, `nestif` 4, `golines` max line 100.
- Do NOT edit the spec file.
- Do NOT commit — dark-factory handles git.
</constraints>

<verification>
Run from the repository root:

```bash
GOFLAGS=-buildvcs=false go test ./... -run 'TestJobWatcherReresolvesNamespaceSet' -v
```

The stdout must contain `--- PASS: TestJobWatcherReresolvesNamespaceSet`.

Then:

```bash
awk '/^func Test/{t=$2; sub(/\(.*/,"",t)} /(Fatalf|Errorf|require\.|assert\.|Expect\()/{c[t]++} END{for(k in c) print k, c[k]}' pkg/job_watcher_namespaces_test.go
grep -c 'jobWatcherNamespaceReresolveInterval' pkg/job_watcher.go
GOFLAGS=-buildvcs=false make precommit VERSION=dev
```

The `awk` must list `TestJobWatcherReresolvesNamespaceSet` with a count ≥ 2. The grep must print a number ≥ 1. `make precommit` must exit 0.
</verification>
