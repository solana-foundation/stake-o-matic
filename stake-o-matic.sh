#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

"$(dirname "$0")"/fetch-release.sh "$STAKE_O_MATIC_RELEASE"

# shellcheck disable=SC2206
TESTNET_ARGS=(
  --url ${URL:?}
  --cluster testnet
  --quality-block-producer-percentage 30
  --max-poor-block-producer-percentage 20
  --max-infrastructure-concentration 25
  --min-epoch-credit-percentage-of-average 35
  --infrastructure-concentration-affects destake-new
  --min-release-version 1.7.3
  --markdown
  $CONFIRM
  stake-pool-v0
  --baseline-stake-amount 5000
  ${RESERVE_ACCOUNT_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
)

# shellcheck disable=SC2206
MAINNET_BETA_ARGS=(
  --url ${URL:?}
  --cluster mainnet-beta
  --quality-block-producer-percentage 30
  --max-poor-block-producer-percentage 20
  --min-epoch-credit-percentage-of-average 35
  --max-active-stake 3000000
  --max-commission 10
  --min-release-version 1.6.14
  --max-infrastructure-concentration 10
  --infrastructure-concentration-affects destake-new
  --min-self-stake 100
  --min-testnet-participation 5 10
  --markdown
  $CONFIRM
  stake-pool-v0
  --baseline-stake-amount 25000
  ${RESERVE_ACCOUNT_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
)

if [[ $BUILDKITE = true ]]; then
  if [[ ! -d db ]]; then
    git clone git@github.com:solana-labs/stake-o-matic.wiki.git db
  fi
  git config --global user.email maintainers@solana.foundation
  git config --global user.name "Solana Maintainers"
fi

if [[ $CLUSTER == "testnet" ]]; then
  ./solana-stake-o-matic "${TESTNET_ARGS[@]}"
elif [[ $CLUSTER == "mainnet-beta" ]]; then
  ./solana-stake-o-matic "${MAINNET_BETA_ARGS[@]}"
else
  echo "CLUSTER must be set to testnet or mainnet-beta"
  exit 1
fi

if [[ $BUILDKITE = true ]]; then
  cd db
  git add ./*
  if ! git diff-index --quiet HEAD; then
    git commit -m "Automated update by $BUILDKITE_BUILD_ID"
    git push origin
  fi
fi
