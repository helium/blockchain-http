#!/usr/bin/env bash

set -euo pipefail

TMPDIR_NAME="scratch"

mkdir -p "$TMPDIR_NAME"

fpm --workdir "$TMPDIR_NAME" \
    -n $(basename $(pwd)) \
    -v $(git describe --long --always) \
    -s dir \
    -t deb \
    _build/prod/rel/=/var/helium
