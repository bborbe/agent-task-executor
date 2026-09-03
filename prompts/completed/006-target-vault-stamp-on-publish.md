---
status: completed
summary: Stamped all published UpdateFrontmatterCommand/IncrementFrontmatterCommand with TargetVault sourced from the task's own target_vault frontmatter key and bumped bborbe/agent to v0.86.0
execution_id: agent-task-executor-target-vault-echo-exec-006-target-vault-stamp-on-publish
dark-factory-version: dev
created: "2026-09-03T17:20:27Z"
queued: "2026-09-03T17:20:27Z"
started: "2026-09-03T17:20:52Z"
completed: "2026-09-03T17:25:49Z"
---
# Stamp target vault on published frontmatter commands

---
status: draft
---

<summary>
- Every frontmatter command the executor publishes names the vault the task belongs to
- The vault comes from the task itself, never from executor configuration
- Tasks without vault information publish exactly as before
- Controllers can use the new field to skip commands that belong to a different vault
- The stamp is covered by tests that capture real published messages
</summary>

<objective>
Populate the new TargetVault routing field on every UpdateFrontmatterCommand and IncrementFrontmatterCommand the executor publishes, sourcing it from the task's own frontmatter, so controllers can skip cross-vault commands before scanning their vault. This is the producer half of the frontmatter-command routing fix; the executor itself is vault-agnostic and must never inject its own vault name.
</objective>

<context>
Read CLAUDE.md for project conventions.

Pattern references (read before writing):
- pkg/result_publisher.go — the file to change; all six command construction sites live here (PublishSpawnNotification, PublishFailure's update + increment pair, PublishIncrementTriggerCount, PublishSetTriggerScope, PublishTypeMismatchFailure)
- pkg/result_publisher_test.go — capturingSyncProducer records published sarama messages; decodeUpdateFrontmatterCommand / decodeIncrementFrontmatterCommand re-unmarshal the real wire payload — reuse these for the new assertions
- lib UpdateFrontmatterCommand / IncrementFrontmatterCommand now carry TargetVault (github.com/bborbe/agent, see CHANGELOG of the release used in the dep bump)
- docs/task-flow-and-failure-semantics.md publisher → command → keys table — TargetVault is a routing field on the command struct, NOT a frontmatter key; do not add it to any Updates map
</context>

<requirements>
1. Bump the dependency: `go get github.com/bborbe/agent@v0.86.0` (the release containing TargetVault on both frontmatter commands). Run this AFTER the code importing the new field exists or in the same step — the import must be written before `go mod tidy` demotes the dep.
2. Add a small helper in pkg/result_publisher.go (e.g. targetVaultFromTask) that reads the task's frontmatter key "target_vault" and returns it as a string, returning "" when absent or not a string.
3. Populate TargetVault from that helper in all six command constructions listed above.
4. Test (pkg/result_publisher_test.go style): publish with a task whose frontmatter carries target_vault: personal → captured update AND increment payloads decode with TargetVault "personal"; publish with a task lacking the key → payloads decode with empty TargetVault (field absent on the wire).
5. Existing publish tests keep passing unchanged where the task fixture has no target_vault key (omitempty keeps the wire shape stable).
6. Add a CHANGELOG.md entry under `## Unreleased` for the routing stamp.
</requirements>

<constraints>
- Do NOT commit — dark-factory handles git
- Existing tests must still pass
- Never use the executor's VAULT_NAME env as the stamp source — the executor serves all vaults ("shared"); only the task's own frontmatter value is correct
- TargetVault is a routing field on the command struct — do not add it to any Updates map or frontmatter write
- Never run `go mod vendor`; use `-mod=mod` for any go test command that needs it
</constraints>

<verification>
Run `make precommit`; must pass.
</verification>
