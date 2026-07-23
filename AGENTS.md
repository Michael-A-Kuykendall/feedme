# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Deficiency Audit & Bead Workflow

A code review of the crate (2026-07-23) surfaced the following deficiencies. Each is
tracked as a bead and chained (B2 is the foundation; the lib.rs-touching fixes follow it,
then the independent CI/docs beads):

| Bead | ID | Deficiency |
|------|------|------------|
| B1 | `feedme-5hc` | `fused` / `invariant-ppt` Cargo features are inert (gate nothing) |
| B2 | `feedme-n8j` | `src/lib.rs` is a 3188-line monolith; needs modularization |
| B3 | `feedme-mua` | `PIIRedaction` is naive: scans all string fields, ignores nesting, not idempotent |
| B4 | `feedme-s9h` | "Validation failures cannot be silently ignored" is opt-in, not absolute |
| B5 | `feedme-zpt` | `fuzz/` excluded, needs nightly, never run in CI |
| B6 | `feedme-1tp` | Provenance + engine scope limits undocumented |

Chain: `feedme-n8j` -> `feedme-5hc` -> `feedme-mua` -> `feedme-s9h` -> `feedme-zpt` -> `feedme-1tp`.

### Branching model (MANDATORY for these beads)

- Trunk is `origin/main` (currently `cd30fa1`). There is no local `main`; branch from `origin/main`.
- Create one overarching integration branch: `deficiency-overhaul` (off `origin/main`).
- For each bead, in chain order:
  1. `git switch -c <bead-id> origin/main` (clean base from trunk)
  2. Implement the fix described in the bead.
  3. Quality gates (ALL must be green before merge):
     - `cargo test --all-features`
     - `cargo clippy --all-features -- -D warnings`
     - `cargo fmt -- --check`
  4. `git rebase deficiency-overhaul` (resolve any conflicts against prior beads), then
     `git switch deficiency-overhaul && git merge --no-ff <bead-id>` (records the merge).
  5. `bd update <bead-id> --status in_progress` while working, `bd close <bead-id>` when merged.
- When all beads are merged into `deficiency-overhaul` and gates are green, open the
  final merge of `deficiency-overhaul` into `origin/main` and push.

Note: `public-release` is the currently checked-out branch but is NOT the trunk; do not
merge into it. Target `origin/main`.

