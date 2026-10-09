# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:970c3bf2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->


## Build & Test

_Add your build and test commands here_

```bash
# Example:
# npm install
# npm test
```

## Architecture Overview

_Add a brief overview of your project architecture_

## Conventions & Patterns

_Add your project-specific conventions here_

## Releases & Container Images

Image: `ghcr.io/hawxxx/kaflux`. Workflows: `.github/workflows/release.yaml` (build, scan, sign, publish) and `release-please.yaml` (versioning).

- Merge to `main` publishes `:main` and `:sha-<commit>`. Never `latest`.
- Releases are cut by merging the **Release PR** that release-please keeps open. It bumps `Chart.yaml`, `docs/openapi.yaml` and `CHANGELOG.md`, tags `vX.Y.Z`, and publishes `X.Y.Z`, `X.Y` and `latest` (only if it is the highest stable version).
- Use Conventional Commits: `fix:` = patch, `feat:` = minor, `feat!:`/`BREAKING CHANGE` = breaking. `chore:`/`docs:` do not release.
- Never hand-edit versions, push `v*` tags, move or retag a version, or merge the Release PR unless the user asks (it publishes publicly). Bad release: ship the next patch.
- Deploy with a pinned version or digest (`0.1.0`, `@sha256:…`), not `latest` or `main`. Compose uses `KAFLUX_VERSION`; Helm defaults to chart `appVersion`.
- `main` is protected by a ruleset: no direct pushes; changes go branch → PR (Conventional Commit title) → green checks (`backend`, `frontend`, `integration`, `packaging`) → merge. Release-please reads only `main`, never branches or open PRs.
