#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

"$(dirname "$0")"/fetch-release.sh "$STAKE_O_MATIC_RELEASE"


if [[ $BUILDKITE = true ]]; then
  if [[ ! -d db ]]; then
    git clone git@github.com:solana-labs/stake-o-matic.wiki.git db
  fi
  git config --global user.email maintainers@solana.foundation
  git config --global user.name "Solana Maintainers"
fi

./solana-stake-o-matic "$@"

if [[ $BUILDKITE = true ]]; then
  cd db
  git add ./*
  if ! git diff-index --quiet HEAD; then
    git commit -m "Automated update by $BUILDKITE_BUILD_ID"
    git push origin
  fi
fi
