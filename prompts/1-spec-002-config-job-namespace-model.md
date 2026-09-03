---
status: draft
spec: [002-config-job-namespace]
created: "2026-09-03T17:50:40Z"
branch: dark-factory/config-job-namespace
---

<summary>
- Config authors can set a `jobNamespace` on a Config to place that agent's Jobs in a namespace other than the executor's.
- Leaving `jobNamespace` unset resolves to a per-environment sandbox namespace — an executor in `dev` resolves `dev-agents-sandbox`, an executor in `prod` resolves `prod-agents-sandbox` — so dev and prod never share a sandbox.
- Setting `jobNamespace` to the executor's own namespace is rejected with an explicit error whose message names the rule, so no Config can opt out of isolation.
- Existing Config CRs stay valid unedited — the new field is additive and optional.
- The resolved namespace value flows end-to-end from the Config CR through the resolver into the runtime configuration object the spawner consumes.
- The new field is registered in the executor's CRD schema so the API server does not prune it (the executor re-applies this schema on every start).
- The resolution rule is pinned by a table-driven test covering the exact cases the spec names: dev+empty, prod+empty, an explicit other-namespace, and the reject case.
</summary>

<objective>
Add the optional `jobNamespace` field to the Config CRD and a `ResolveJobNamespace` derivation/rejection function so the executor can later spawn Jobs outside its own namespace, defaulting to the safe per-environment sandbox and refusing any attempt to use the executor's own namespace. This prompt makes the field exist and resolve correctly end-to-end; the spawner wiring that consumes the resolved value ships in the next prompt.
</objective>

<context>
Read `/home/node/.claude/CLAUDE.md` for project conventions (Ginkgo/Gomega v2, `github.com/bborbe/errors` wrapping, glog V(n) gating, coverage ≥80% for new code).

Read these files fully before changing anything:
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go` — `ConfigSpec` struct (the `Equal` method and `Validate` method at the bottom), the existing field-declaration style (doc comment + json tag, e.g. `PriorityClassName string \`json:"priorityClassName,omitempty"\``), the `errors` import already present.
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types_test.go` — mirror the existing per-field `Describe` blocks exactly: "JSON round-trip for priorityClassName" (~line 225) and "Equal with priorityClassName" (~line 252). The file is `package v1_test` with a Ginkgo suite entry `TestV1`.
- `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/zz_generated.deepcopy.go` — confirm `ConfigSpec.DeepCopyInto` does `*out = *in` first; a new `string` field needs NO generator run.
- `/workspace/pkg/agent_configuration.go` — `AgentConfiguration` struct (note: it carries `PriorityClassName`, `Trigger`, `ZombieJobTimeoutSeconds` — fields mirrored from `ConfigSpec`) and `TaggedConfigurations` which copies every field explicitly.
- `/workspace/pkg/config_resolver.go` — the `convert` function (copies `ConfigSpec` fields into `AgentConfiguration`).
- `/workspace/pkg/k8s_connector.go` — the `configSpecSchema` / `configSpecProperties` region (the doc comment above `configSpecSchema` explains why every field MUST be registered here or the API server prunes it; see the v0.5.0 `maxConcurrentJobs` incident note).
- `/workspace/pkg/config_resolver_test.go` — the first `It("returns converted AgentConfiguration with image tag appended", ...)` which asserts each converted field.
- `/workspace/pkg/agent_configuration_test.go` — the `Describe("TaggedConfigurations", ...)` block's field-preservation specs.

Relevant docs (in-container paths):
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-patterns.md` — public API + private struct conventions, error wrapping.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-error-wrapping-guide.md` — never `fmt.Errorf`; `errors.Errorf(ctx, ...)` for a new error without a cause.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-testing-guide.md` — Ginkgo/Gomega, coverage ≥80% for new code, external vs internal test packages.
- `/home/node/.claude/plugins/marketplaces/coding/docs/go-kubernetes-crd-controller-guide.md` — CRD field registration patterns.
</context>

<requirements>
The end state: the Config CRD carries an optional `jobNamespace`, a `ResolveJobNamespace` function implements the spec's resolution rule (empty → `<executorNamespace>-agents-sandbox`, explicit value → that value, executor's own namespace → error), the value flows through the resolver into `AgentConfiguration`, and the executor's CRD schema registers the field so the API server keeps it.

1. Add the `JobNamespace` field to `ConfigSpec` in `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types.go`. Insert it after `PriorityClassName` (both are Job-stamping fields) with the same style as the neighbouring fields:
   ```go
   // JobNamespace is the Kubernetes namespace the agent's Jobs are spawned into.
   // Optional; when empty, the executor resolves a per-environment sandbox
   // namespace ("<executorNamespace>-agents-sandbox") so dev and prod never
   // share a sandbox. Setting it to the executor's own namespace is rejected at
   // spawn time — no Config can opt out of isolation.
   JobNamespace string `json:"jobNamespace,omitempty"`
   ```

2. Add `s.JobNamespace == o.JobNamespace` to `ConfigSpec.Equal` in the same file, directly after the `s.PriorityClassName == o.PriorityClassName` line.

3. Add the resolution function to the same file, after the last helper `validateTaskTypesList` (package-level, since it needs the executor namespace which `Validate` does not know — the rejection is a runtime, per-executor guard, NOT an admission check; do NOT call it from `Validate`):
   ```go
   // ResolveJobNamespace returns the namespace an agent's Jobs must be spawned
   // into. An empty jobNamespace resolves to "<executorNamespace>-agents-sandbox"
   // (derived per environment so dev and prod never share a sandbox); any other
   // value is returned unchanged; a jobNamespace equal to the executor's own
   // namespace is rejected so no Config can opt out of isolation.
   func ResolveJobNamespace(ctx context.Context, jobNamespace string, executorNamespace string) (string, error) {
       if jobNamespace == "" {
           return executorNamespace + "-agents-sandbox", nil
       }
       if jobNamespace == executorNamespace {
           return "", errors.Errorf(
               ctx,
               "jobNamespace %q may not equal the executor namespace %q",
               jobNamespace,
               executorNamespace,
           )
       }
       return jobNamespace, nil
   }
   ```
   The message MUST contain the exact substring `may not equal the executor namespace` — the spec's acceptance test asserts on it. `errors` is already imported in this file.

4. Create `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/job_namespace_test.go` in `package v1_test` (a plain stdlib test file — the spec's acceptance criterion requires `--- PASS: TestResolveJobNamespace` output, which a Ginkgo `It` would not produce; the existing Ginkgo suite in `types_test.go` is untouched). Table-driven test with exactly these cases:
   ```go
   func TestResolveJobNamespace(t *testing.T) {
       tests := []struct {
           name              string
           jobNamespace      string
           executorNamespace string
           want              string
           wantErrSubstring  string
       }{
           {"empty resolves to dev-agents-sandbox", "", "dev", "dev-agents-sandbox", ""},
           {"empty resolves to prod-agents-sandbox", "", "prod", "prod-agents-sandbox", ""},
           {"explicit other-ns is returned unchanged", "other-ns", "dev", "other-ns", ""},
           {"executor namespace is rejected", "dev", "dev", "", "may not equal the executor namespace"},
       }
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               got, err := agentv1.ResolveJobNamespace(context.Background(), tt.jobNamespace, tt.executorNamespace)
               if tt.wantErrSubstring != "" {
                   if err == nil {
                       t.Fatalf("expected error containing %q, got nil", tt.wantErrSubstring)
                   }
                   if !strings.Contains(err.Error(), tt.wantErrSubstring) {
                       t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErrSubstring)
                   }
                   return
               }
               if err != nil {
                   t.Fatalf("unexpected error: %v", err)
               }
               if got != tt.want {
                   t.Fatalf("ResolveJobNamespace(%q, %q) = %q, want %q", tt.jobNamespace, tt.executorNamespace, got, tt.want)
               }
           })
       }
   }
   ```
   Imports: `context`, `strings`, `testing`, and `agentv1 "github.com/bborbe/agent-task-executor/k8s/apis/agent.benjamin-borbe.de/v1"`.

5. Add two Ginkgo `Describe` blocks to `/workspace/k8s/apis/agent.benjamin-borbe.de/v1/types_test.go`, mirroring the `priorityClassName` pattern verbatim:
   - `Describe("JSON round-trip for jobNamespace")` with two `It`s: (a) a `ConfigSpec` with `JobNamespace: "other-ns"` round-trips — marshal, unmarshal, `Expect(decoded.JobNamespace).To(Equal("other-ns"))`; (b) a `ConfigSpec` without `JobNamespace` marshals WITHOUT the key — `Expect(string(data)).NotTo(ContainSubstring("jobNamespace"))` (this pins the `omitempty` / additive constraint).
   - `Describe("Equal with jobNamespace")` with two `It`s: (a) returns false when only `JobNamespace` differs; (b) returns true when `JobNamespace` matches.

6. Add the field to the runtime config type in `/workspace/pkg/agent_configuration.go`:
   - Add to the `AgentConfiguration` struct, after `PriorityClassName`, with a doc comment matching the neighbouring fields:
     ```go
     // JobNamespace is the Kubernetes namespace the agent's Jobs are spawned
     // into. Empty means the executor derives a per-environment sandbox
     // namespace ("<executorNamespace>-agents-sandbox") at spawn time.
     JobNamespace string
     ```
   - Add `JobNamespace: c.JobNamespace,` to the struct literal inside `TaggedConfigurations` (every field is copied explicitly there — do not forget this or the tagged copy drops the value).

7. Propagate the field through the resolver in `/workspace/pkg/config_resolver.go`: in the `AgentConfiguration{...}` literal inside `convert`, add `JobNamespace: obj.Spec.JobNamespace,` (next to `PriorityClassName: obj.Spec.PriorityClassName,`).

8. Register the field in the executor's CRD schema in `/workspace/pkg/k8s_connector.go`: inside `configSpecProperties()`, add `"jobNamespace": {Type: "string"},` next to the `"secretName"` / `"volumeClaim"` entries. The doc comment above `configSpecSchema` is authoritative: an unregistered field is silently pruned by the API server from every Config within seconds of the next executor start — this step is what makes the field real in production.

9. Create `/workspace/pkg/k8s_connector_internal_test.go` in `package pkg` (internal test file — the schema funcs are unexported; this mirrors the `pkg/job_watcher_internal_test.go` precedent of an internal test file). This is the boundary test that guards against the silent-prune regression:
   ```go
   func TestConfigSpecSchemaIncludesJobNamespace(t *testing.T) {
       props := configSpecProperties()
       p, ok := props["jobNamespace"]
       if !ok {
           t.Fatal("configSpecProperties() missing jobNamespace — the API server will prune the field from every Config on next restart")
       }
       if p.Type != "string" {
           t.Fatalf("jobNamespace schema type = %q, want string", p.Type)
       }
   }
   ```

10. Extend `/workspace/pkg/config_resolver_test.go`: in the first `It("returns converted AgentConfiguration with image tag appended", ...)`, add `JobNamespace: "sandbox-ns"` to the `ConfigSpec` literal and assert `Expect(config.JobNamespace).To(Equal("sandbox-ns"))` next to the other converted-field assertions. This is the boundary test proving the field crosses the resolver (Config CR → `AgentConfiguration`).

11. Extend `/workspace/pkg/agent_configuration_test.go`: inside `Describe("TaggedConfigurations", ...)`, add `It("preserves JobNamespace", ...)` that tags `configs` and asserts `tagged[0].JobNamespace` equals the source value (add `JobNamespace: "sandbox-ns"` to the package-level `configs` fixture used by those specs if it is not already set there).

12. Do NOT run any code generator. `ConfigSpec.DeepCopyInto` copies the struct first (`*out = *in`), which already covers a plain `string` field — `zz_generated.deepcopy.go` needs no change. Do NOT add `JobNamespace` to `ConfigSpec.Validate` — the executor-namespace rejection is a runtime per-executor guard, not a CRD admission rule (the same CRD is shared across dev/prod executors).
</requirements>

<constraints>
- Existing `Config` CRs must remain valid unedited — `JobNamespace` is additive and optional (the JSON `omitempty` test in requirement 5 pins this).
- The default must be the safe value: omitting the field may never place a Job in the executor's namespace (the reject case in requirement 4 pins this).
- No new capability, securityContext change, or RBAC grant is introduced by this spec.
- `TaskType`/`TaskTypes` routing behavior is untouched.
- Do NOT call `ResolveJobNamespace` from `ConfigSpec.Validate` or `ConfigSpec.Config.Validate` — the executor namespace is a per-executor runtime value unknown at admission time.
- Do NOT change the `JobSpawner` interface, the `NewJobSpawner` constructor, `pkg/factory/factory.go`, `main.go`, or any signature — the spawner wiring is the next prompt.
- This prompt does NOT touch `/workspace/pkg/spawner/job_spawner.go` — that is the next prompt.
- Do NOT commit — dark-factory handles git.
- Existing tests must still pass.
</constraints>

<verification>
Run `make test` iteratively after each meaningful change (fast feedback loop), then `make precommit` ONCE at the very end.

- `make test` — exits 0.
- `make precommit` — exits 0.
- `go test ./... -run TestResolveJobNamespace -v` — stdout contains `--- PASS: TestResolveJobNamespace` (and each `=== RUN`/`--- PASS` subtest line).
- `go test -mod=mod -coverprofile=/tmp/cover.out ./k8s/apis/agent.benjamin-borbe.de/v1/... ./pkg/... && go tool cover -func=/tmp/cover.out` — `ResolveJobNamespace` is fully covered (the table test hits empty / passthrough / reject branches), and the new `convert` / `TaggedConfigurations` / schema assertions each execute at least once (≥80% statement coverage for new code).

Do NOT run `docker`, `make build`, `kubectl`, `dark-factory`, or `git` commands in this prompt — the container cannot execute them and the daemon does not check their exit codes.
</verification>
