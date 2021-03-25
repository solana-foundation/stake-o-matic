#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

"$(dirname "$0")"/fetch-release.sh "$STAKE_O_MATIC_RELEASE"
exec ./solana-stake-o-matic "$@"
