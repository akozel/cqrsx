#!/usr/bin/env bash
# Backlog epic 06: a back-end image of the stand from ../Dockerfile (the demo's box office and the synthetic module
# `probe` of ../backend), built before a series and never during one.
#   build.sh <tag> <journal>            encore-load-backend:<tag> from the working tree as it is (e06-03: the tree after
#                                       ticket 03, before its commit)
#   build.sh <tag> <journal> <commit>   from `git archive <commit>` with this directory (demo/load/epic06/) of the working
#                                       tree laid over it: the engine and the demo of that commit, the probe of today
#                                       (e06-base: a270a06, the HEAD before the epic's code changes)
# The Dockerfile touches our sources before cargo, so the shared build cache never lets a stale binary through. The
# journal gets the HEAD or the commit, the build inputs that differ from it and the image id.
set -euo pipefail
export DOCKER_HOST="${DOCKER_HOST:-unix:///run/podman/podman.sock}"
export PATH=$HOME/.local/bin:$PATH
EPIC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "$EPIC/../../.." && pwd)"
SCRATCH="${SCRATCH:-/tmp/claude-1000/-workspace/0baccf55-8936-4ac7-8c8d-72d2c7689858/scratchpad/e06}"
INPUTS=(cqrsx cqrsx-macros demo/Cargo.toml demo/back-end demo/sidecar demo/load/epic06 rust-toolchain.toml rustfmt.toml .dockerignore)
if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: build.sh <tag> <journal> [<commit>]" >&2
  exit 2
fi
TAG="$1" JOURNAL="$2" COMMIT="${3:-}"
mkdir -p "$SCRATCH" "$(dirname "$JOURNAL")"
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$JOURNAL"; }

if [[ -n "$(docker ps -q --filter label=com.docker.compose.project=encore-load 2>/dev/null)" ]]; then
  say "build: a stand of the project encore-load is up: no build during a run"
  exit 2
fi
if [[ -n "$COMMIT" ]]; then
  context="$SCRATCH/src-$COMMIT"
  rm -rf "$context" && mkdir -p "$context"
  git -C "$ROOT" archive "$COMMIT" | tar -x -C "$context"
  # This directory as it is in the working tree (its target/ aside), whatever the commit has of it.
  rm -rf "$context/demo/load/epic06" && mkdir -p "$context/demo/load"
  tar -C "$ROOT/demo/load" --exclude=epic06/target -cf - epic06 | tar -x -C "$context/demo/load"
  say "build: $TAG from git archive $COMMIT ($(git -C "$ROOT" rev-parse --short "$COMMIT")) with demo/load/epic06 of the working tree; its changes against $COMMIT:"
  overlay="$(git -C "$ROOT" status --porcelain -- demo/load/epic06; git -C "$ROOT" diff --stat "$COMMIT" -- demo/load/epic06)"
  echo "${overlay:-  none}" | tee -a "$JOURNAL"
else
  context="$ROOT"
  dirty="$(git -C "$ROOT" status --porcelain -- "${INPUTS[@]}")"
  say "build: $TAG from the working tree, HEAD $(git -C "$ROOT" rev-parse --short HEAD); build inputs that differ from it:"
  echo "${dirty:-  none}" | tee -a "$JOURNAL"
fi
log="$SCRATCH/build-$TAG.log"
start=$SECONDS
docker build -f "$context/demo/load/epic06/Dockerfile" -t "encore-load-backend:$TAG" "$context" > "$log" 2>&1 \
  || { say "build: $TAG failed (see $log)"; exit 1; }
say "build: encore-load-backend:$TAG in $((SECONDS - start)) s"
say "image encore-load-backend:$TAG $(docker image inspect -f '{{.Id}} created {{.Created}}' "encore-load-backend:$TAG")"
