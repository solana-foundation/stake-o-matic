#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

"$(dirname "$0")"/fetch-release.sh
exec ./solana-stake-o-matic "$@"
