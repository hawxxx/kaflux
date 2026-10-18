"""Select required CI jobs; unknown inputs conservatively select every job."""

import os
import subprocess
from pathlib import Path

JOBS = ("backend", "frontend", "integration", "packaging")


def select_jobs(paths):
    selected = set()
    for path in paths:
        if path == "docs/openapi.yaml":
            selected.add("packaging")
        elif path.endswith(".md") or path.startswith("docs/"):
            continue
        elif path.startswith("backend/"):
            selected.update(("backend", "integration", "packaging"))
        elif path.startswith("frontend/"):
            selected.update(("frontend", "packaging"))
        elif path in ("Dockerfile", ".dockerignore") or path.startswith("deploy/helm/"):
            selected.add("packaging")
        elif path == "docker-compose.yaml" or path.startswith("deploy/"):
            selected.update(("integration", "packaging"))
        else:
            return set(JOBS)
    return selected


def main():
    selected = set(JOBS)
    if os.environ.get("EVENT") == "pull_request":
        try:
            # Disable rename detection so moving code into docs still tests its deletion.
            diff = subprocess.check_output([
                "git", "diff", "--name-only", "--no-renames", "-z",
                f'{os.environ["BASE_SHA"]}...{os.environ["HEAD_SHA"]}',
            ])
            selected = select_jobs(os.fsdecode(diff).rstrip("\0").split("\0"))
        except (OSError, subprocess.CalledProcessError, KeyError) as error:
            print(f"Change detection failed; running all checks: {error}")
    output = "".join(f"{job}={str(job in selected).lower()}\n" for job in JOBS)
    print(output, end="")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        stream.write(output)


if __name__ == "__main__":
    main()
