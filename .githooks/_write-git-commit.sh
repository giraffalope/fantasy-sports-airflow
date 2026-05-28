#!/bin/sh -e
# Shared worker for the post-* hooks in this directory. Writes
#   GIT_COMMIT=<HEAD sha>
# into <repo-root>/.git-commit.env using a temp-file + mv so a concurrent
# docker-compose up never reads a half-written file. Docker Compose forwards
# this file into every Airflow service via the env_file: block on the
# &airflow-common anchor, so the ML pipeline_version cache-key tracks HEAD
# automatically (see docs/nascar/cup.machine_learning.models.md).

root="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$root" ]; then
    # Not inside a worktree (e.g. invoked during a partial clone). Bail
    # quietly rather than failing the surrounding git op.
    exit 0
fi

sha="$(git -C "$root" rev-parse HEAD 2>/dev/null || true)"
if [ -z "$sha" ]; then
    # Detached state with no commits yet (fresh init, mid-rebase, etc.).
    exit 0
fi

out="${root}/.git-commit.env"
tmp="$(mktemp "${out}.XXXXXX")"
printf 'GIT_COMMIT=%s\n' "$sha" > "$tmp"
mv "$tmp" "$out"
