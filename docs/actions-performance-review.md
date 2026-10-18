# GitHub Actions performance review

Reviewed 2026-10-18 using the workflows on GitHub `main` and recent completed runs.
The local `feat/cluster-version` checkout predates improvements already on `main`;
the CI changes below incorporate the existing main-only push policy and docs
filtering, with component selection replacing the broader docs-only filter.

## Implemented optimizations

- PRs run checks for affected components; `main` pushes run all four checks.
  Frontend changes run frontend and packaging; backend changes run backend,
  integration and packaging. Helm, Dockerfile and OpenAPI changes run packaging;
  Compose and other deploy changes run integration and packaging. Unknown paths
  and detection failures run everything. Renames account for removed inputs.
- Superseded PR runs are cancelled. Required check names remain unchanged, and
  irrelevant jobs report skipped rather than leaving a filtered workflow pending.
- Packaging uses Buildx with the separate `ci-packaging` GitHub Actions layer
  cache, Google's Docker Hub mirror, and a locally loaded smoke-test image.
  Cache export failure does not fail an otherwise successful build.
- CI jobs have bounded timeouts. The image smoke test, race tests, vulnerability
  checks and browser checks remain in place; release publication is unchanged.

Branch policy remains selective PR checks and full checks on `main`. Restricting
checks to release branches would allow untested changes to reach `main` and is
not enabled. Build timings and cold/warm cache savings require hosted CI runs.

## Measured costs

Three successful CI runs sampled from October 16:

| Job or step | Observed elapsed time |
| --- | --- |
| Packaging job | 242–325 seconds |
| Packaging `docker build` | 217–299 seconds |
| Frontend job | 209–319 seconds |
| Frontend `npm run build` | 140–227 seconds |
| Backend job | 87–102 seconds |
| Integration job | 92–111 seconds |

Jobs run in parallel, so these times cannot be added to estimate PR wait time.
The sampled CI runs used approximately 11–14 aggregate job minutes each,
excluding queue time and billing rounding. This is compute usage, not a bill estimate.

Evidence: [PR CI](https://github.com/hawxxx/kaflux/actions/runs/38975641713),
[main CI](https://github.com/hawxxx/kaflux/actions/runs/38975603906),
[another PR CI](https://github.com/hawxxx/kaflux/actions/runs/38973899610).

## Recommended order

1. **Cache the packaging Docker build.** CI uses plain `docker build` on a fresh
   runner. Use Buildx with the GitHub Actions cache backend, an explicit CI cache
   scope, and `load: true` for the existing container smoke test. The release
   workflow already has a remote build cache. Preserve the smoke test and measure
   cold and warm builds; Dockerfile cache mounts alone do not persist across fresh
   runners, and layer-cache export does not automatically preserve their contents.
2. **Cancel superseded PR CI runs.** Add a workflow concurrency group keyed by
   workflow and PR number (falling back to ref), with cancellation enabled for
   pull requests. Keep release publication concurrency unchanged. Savings depend
   on how often new commits arrive while CI is running.
3. **Filter jobs by affected components.** Current `main` skips docs-only PR
   builds, but every other change still runs all four jobs. Frontend-only changes
   can skip backend and integration; backend-only changes can skip standalone
   frontend checks. Keep packaging for changes to either application, Dockerfile,
   or relevant packaging inputs. Map Compose, API spec, Helm, dependency and
   workflow changes explicitly, and run everything if change detection fails.
   Preserve the required job names: backend, frontend, integration, packaging.
   Skip individual jobs, rather than the entire required workflow, so branch
   protection receives a result.
4. **Profile the frontend build.** Time `tsc -b` and `vite build` separately before
   choosing an optimization. The frontend is built both in its check job and
   inside the packaging image. Caching unchanged layers is the first step;
   artifact reuse would require a deliberate change to image build inputs and
   validation. Do not remove type checking or browser verification.
5. **Bound stalled CI jobs.** Add job timeouts based on observed runtime with
   sufficient cold-cache headroom. This limits exceptional waste rather than
   making successful runs faster.

## Already in place

- CI on `main` pushes and pull requests, avoiding duplicate branch-push CI.
- Docs-only PR filtering at job level, including an OpenAPI exception.
- Go dependency/build caching through setup-go and npm download caching.
- Release Buildx cache, multi-platform cross-compilation, and promotion of the
  scanned digest without rebuilding for public tags.

Browser installation was not a leading cost in these samples; prioritize measured
build bottlenecks over adding a browser cache. Main CI also verifies the merged
result, so removing it would trade away coverage and is not recommended here.

## Validation and limits

Read all four local workflows and checked the current CI and release configuration
through the GitHub API. Inspected job and step timestamps for the linked runs and
one release run. No workflows were modified or dispatched, and no builds or tests
were run for the initial read-only review. The implementation was subsequently
checked with Python unittest coverage for selection, failure and rename behavior,
workflow linting, and whitespace validation. Savings are not benchmarked yet.

GitHub documents [concurrency cancellation](https://docs.github.com/en/actions/concepts/workflows-and-actions/concurrency)
and the [pending-check behavior of workflow filters](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).
Docker documents the [GitHub Actions cache backend](https://docs.docker.com/build/cache/backends/gha/).
