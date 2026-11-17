---
status: completed
summary: Delete a failed or timed-out build Job right after printing its logs, lower build-job ttlSecondsAfterFinished to 46800, and add the Unreleased changelog entry
execution_id: agent-task-executor-delete-failed-build-job-exec-064-delete-failed-build-job
dark-factory-version: v0.198.17
created: "2026-11-09T21:45:00Z"
queued: "2026-11-09T21:36:59Z"
started: "2026-11-09T22:11:16Z"
completed: "2026-11-09T22:19:33Z"
---

# Delete the build Job after its logs are printed

<summary>
- A failed or timed-out cluster build no longer leaves its Job and pod behind
- The build's last log lines are still printed before the Job is removed
- One failed build therefore stops holding the K8sPodStatus critical alert, which needs a retained failed pod to fire
- Successful builds keep their Job, so the image digest can still be read from its logs
- The window before a retained Job is cleaned up is shortened to the 13 hours the cluster-build guide prescribes
</summary>

<objective>
Remove a failed or timed-out build Job immediately after `hack/build-cluster.sh` prints its logs, so a single failed build stops holding the `K8sPodStatus` critical alert for a day, and shorten the Job retention window to 46800 seconds (13 hours) — the canonical value from the cluster-build guide, which is a vault page and not in this repo, so treat the number as given.
</objective>

<context>
Read `docs/dod.md` for the project's definition of done.

Read `hack/build-cluster.sh`. The success path is the `if [ "${outcome}" = succeeded ]` branch: it prints the digest and exits, so anything placed after it cannot run on success. The failure path prints `logs --tail=100 >&2` and then leaves through `die`, once for `timeout` and once for `failed`.

`kc` is the kubectl wrapper defined in `hack/cluster-alias.sh`, which the script sources near the top. Every kubectl call in the script goes through `kc`, so the target cluster follows `BUILD_TARGET`; do not introduce a second client, a literal namespace, or a `--kubeconfig` flag.

Read `hack/build-job.yaml` — its `ttlSecondsAfterFinished` is `86400`.

Read `CHANGELOG.md` — it has no `## Unreleased` section; the newest released section is the first `## v` heading.
</context>

<requirements>
1. In `hack/build-cluster.sh`, immediately after the line `logs --tail=100 >&2`, add this line at **column 0** — it is a top-level statement, like the `logs` line above it, not part of the `if` block below:

   `kc -n "${NAMESPACE}" delete job "${JOB_NAME}" --ignore-not-found || echo "build-cluster: could not delete job ${NAMESPACE}/${JOB_NAME}" >&2`

   It must sit after the log print and before the `if [ "${outcome}" = timeout ]` block, so that both the `timeout` and the `failed` exits delete the Job, and so that it cannot run on the success path.

   The `|| echo …` is required: the script runs under `set -euo pipefail`, so a bare failing `kc` would abort the script before the `die` lines and replace the build-failure message with a raw kubectl error.
2. In `hack/build-job.yaml`, change `ttlSecondsAfterFinished: 86400` to `ttlSecondsAfterFinished: 46800`.
3. In `CHANGELOG.md`, add a `## Unreleased` section directly above the newest `## v` heading (below the semver preamble) containing one bullet:
   `- fix: delete a failed or timed-out build Job after printing its logs, so a single failure no longer holds the K8sPodStatus alert; align build-job ttlSecondsAfterFinished to 46800`
4. Before finishing, re-run `<verification>` and confirm it passes, then walk each requirement above against the change.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git.
- Do NOT change the success path: the `outcome = succeeded` branch must keep the Job so its digest can still be read.
- Do NOT modify `hack/cluster-alias.sh`, any other field of `hack/build-job.yaml`, or the alert rule.
- The script's existing behaviour and exit codes are unchanged apart from the added delete.
</constraints>

<verification>
Run `ROOTDIR=/workspace make precommit` — must pass.

Then confirm the placement, which precommit does not cover:
- `bash -n hack/build-cluster.sh` exits 0
- `[ "$(grep -c 'delete job "${JOB_NAME}" --ignore-not-found' hack/build-cluster.sh)" = 1 ]` — the pre-existing hint text on an earlier line reads `delete job ${JOB_NAME}` with no flags, so it does not match
- `[ "$(grep -c 'ttlSecondsAfterFinished: 46800' hack/build-job.yaml)" = 1 ]`
- `[ "$(grep -c 'ttlSecondsAfterFinished: 86400' hack/build-job.yaml)" = 0 ]`
- `awk '/^## /{sec=$0} /delete a failed or timed-out build Job/{print sec}' CHANGELOG.md` prints `## Unreleased`
- `[ "$(grep -c '^## Unreleased$' CHANGELOG.md)" = 1 ]`
</verification>
