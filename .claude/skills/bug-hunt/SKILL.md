---
name: bug-hunt
description: Hunts for correctness, concurrency, and security bugs in clicko, a ClickHouse migration tool. Cross-references README.md (documented/expected behavior) and IMPROVEMENT.md (backlog of past review rounds, including declined/deferred items with reasons) so it never re-reports known or already-decided issues, then appends new findings to IMPROVEMENT.md in the repo's established round format. Use when asked to find bugs, do a bug hunt, run a review round, or update clicko's improvement backlog.
---

# Clicko bug hunt

Repo-specific bug hunt for clicko. README.md and IMPROVEMENT.md are required reading before touching any code — they are the ground truth for "documented behavior" and "already-known issues," and this skill's whole job is to not contradict or duplicate them.

## Required reading (every run, in order)

1. **README.md** — the contract: documented flags, guarantees, best practices. Anything the code does that contradicts this is a candidate bug (or, if the code is right and the docs are wrong/silent, a docs bug — see step 7).
2. **IMPROVEMENT.md** — the backlog: numbered findings from prior rounds, each ranked by severity, plus explicit **declined/deferred** items with their reasoning. Do not re-report a declined/deferred item unless new evidence changes the tradeoff its decision was based on — if it does, say so explicitly and cite which past item it revises (see the "Round 3" entry for the template: it explicitly says it reverses item #17 and why).

## Scope

Go source: `loader.go`, `migrator.go`, `store.go`, `register.go`, `dryrun.go`, `clicko.go`, `type.go`, `cmd/`, `internal/`. Treat `dev/` and `example/` as setup only, not bug-hunt targets, unless the task is specifically about them.

## What to hunt for (clicko-specific)

- **No transactional DDL, no advisory locks in ClickHouse.** Any new logic that assumes atomicity across statements, or that two `clicko` processes won't race, is suspect — check it's either handled or knowingly deferred like existing items #1/#2.
- **`ON CLUSTER` / identifier interpolation.** Anything building SQL with cluster, table, column, or engine names must be regex-validated or backtick-quoted. Flag raw string concatenation into SQL.
- **Credential handling.** URIs/DSNs must never leak via logs or error messages — check `redactCredentials` covers every path that could embed the URI, including new error paths.
- **Migration ordering/versioning.** Forward-only skip semantics, `--allow-out-of-order`, duplicate version rows, applied-version-vs-file-on-disk mismatches (deleted/edited migration files).
- **Dry-run purity.** `--dry-run` must perform zero writes, including implicit ones (tracking table creation, side-effect queries inside Go migrations).
- **Flag-to-SQL validation.** `--insert-quorum`, `--engine`, and similar user-supplied values reaching SQL unvalidated.
- **Partial-failure state.** Crash/error between `conn.Exec` and `store.Add`, signal handling mid-migration.

## Process

1. Read README.md and IMPROVEMENT.md in full.
2. Find the next round number: highest `## Round N` in IMPROVEMENT.md, plus one. Use today's actual date.
3. Review the code (or the diff, if the task is reviewing a specific change) against the categories above plus general correctness.
4. For each candidate finding, cross-check it against IMPROVEMENT.md's existing numbered items and declined/deferred notes. Skip anything already covered; call out anything that supersedes or narrows a prior decision.
5. Rank findings by severity within the round.
6. Append a new `## Round N (YYYY-MM-DD)` section to IMPROVEMENT.md: numbered findings continuing from the highest existing `#`, and a short "Verified clean" note listing what was specifically checked and found fine (matches the style of the existing rounds).
7. If a finding shows README.md is wrong, misleading, or silent about real behavior uncovered during the hunt, update README.md too and note that in the IMPROVEMENT.md entry.
8. This skill only finds and records — don't fix anything unless explicitly asked. If asked to fix in the same pass, use a single branch for the whole round, not one branch per item (this repo's convention).

## Output

- Updated `IMPROVEMENT.md` with the new round: correctly numbered, ranked by severity, cross-referenced against prior decisions.
- Updated `README.md` only if a documented-behavior/docs bug was found.
- A short chat summary: round number, count of new findings, and any explicit reversal of a prior round's decision.
