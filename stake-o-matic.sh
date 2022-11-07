#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

DIR="$(dirname "$0")"

"$DIR"/fetch-release.sh "$STAKE_O_MATIC_RELEASE"

if [[ -n $FOLLOWER ]]; then
  REQUIRE_CLASSIFICATION="--require-classification"
else
  CSV_OUTPUT_MODE="--csv-output-mode first"
  EPOCH_CLASSIFICATION="--epoch-classification first"
fi

if [[ -n $NO_WIKI_OUTPUT ]]; then
  CSV_OUTPUT_MODE="--csv-output-mode no"
  EPOCH_CLASSIFICATION="--epoch-classification no"
fi

if [[ -n $SHORT_TESTNET_PARTICIPATION ]]; then
  TESTNET_PARTICIPATION="--min-testnet-participation 2 4"
else
  TESTNET_PARTICIPATION="--min-testnet-participation 5 10"
fi

if [[ -n $BLOCKLIST_DATACENTER_ASNS ]]; then
  BLOCKLIST="--blocklist-datacenter-asns ${BLOCKLIST_DATACENTER_ASNS}"
fi

if [[ ! -d db ]]; then
  git clone git@github.com:solana-labs/stake-o-matic.wiki.git db
fi

# shellcheck disable=SC2206
TESTNET_ARGS=(
  --url ${URL:?}
  --participant-url ${PARTICIPANT_URL:?}
  --cluster testnet
  --quality-block-producer-percentage 30
  --max-poor-block-producer-percentage 20
  --max-infrastructure-concentration 25
  --min-epoch-credit-percentage-of-average 35
  --infrastructure-concentration-affects destake-new
  --min-release-version 1.14.4
  --max-old-release-version-percentage 20
  --performance-db-url ${PERFORMANCE_DB_URL:?}
  --performance-db-token ${PERFORMANCE_DB_TOKEN:?}
  --require-performance-metrics-reporting
)

# shellcheck disable=SC2206
MAINNET_BETA_ARGS=(
  --url ${URL:?}
  --participant-url ${URL:?}
  --cluster mainnet-beta
  --quality-block-producer-percentage 30
  --max-poor-block-producer-percentage 20
  --min-epoch-credit-percentage-of-average 35
  --max-active-stake 3000000
  --max-commission 10
  --min-release-version 1.13.3
  --max-infrastructure-concentration 10
  --infrastructure-concentration-affects destake-new
  --min-self-stake 100
  $TESTNET_PARTICIPATION
  --enforce-testnet-participation
  --enforce-min-self-stake
  --min-self-stake-exceptions-file ${DIR:?}/assets/exclude.yml.enc
  --min-self-stake-exceptions-key ${SELF_STAKE_EXCEPTIONS_KEY:?}
  --performance-db-url ${PERFORMANCE_DB_URL:?}
  --performance-db-token ${PERFORMANCE_DB_TOKEN:?}
  --require-performance-metrics-reporting
)

# shellcheck disable=SC2206
NOT_A_STAKE_POOL_ARGS=(
  $BLOCKLIST
  $CSV_OUTPUT_MODE
  $EPOCH_CLASSIFICATION
  $CONFIRM
  $REQUIRE_CLASSIFICATION
  stake-pool-v0
  --min-reserve-stake-balance ${MIN_RESERVE_STAKE_BALANCE:?}
  ${RESERVE_ACCOUNT_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
  ${BASELINE_STAKE_AMOUNT:?}
)

# shellcheck disable=SC2206
STAKE_POOL_ARGS=(
  $BLOCKLIST
  $EPOCH_CLASSIFICATION
  $CONFIRM
  $REQUIRE_CLASSIFICATION
  stake-pool
  ${STAKE_POOL_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
  ${BASELINE_STAKE_AMOUNT:?}
)

if [[ $CLUSTER = "testnet-stake-pool" ]]; then
  ./solana-stake-o-matic "${TESTNET_ARGS[@]}" "${STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER = "mainnet-beta-stake-pool" ]]; then
  ./solana-stake-o-matic "${MAINNET_BETA_ARGS[@]}" "${STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER == "testnet" ]]; then
  ./solana-stake-o-matic "${TESTNET_ARGS[@]}" "${NOT_A_STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER == "mainnet-beta" ]]; then
  ./solana-stake-o-matic "${MAINNET_BETA_ARGS[@]}" "${NOT_A_STAKE_POOL_ARGS[@]}"
else
  echo "CLUSTER must be set to testnet-stake-pool, mainnet-beta-stake-pool, testnet, or mainnet-beta"
  exit 1
fi

if [[ -z $FOLLOWER && $BUILDKITE = true ]]; then
  git config --global user.email maintainers@solana.foundation
  git config --global user.name "Solana Maintainers"
  cd db
  git add ./*
  if ! git diff-index --quiet HEAD; then
    git commit -m "Automated update by $BUILDKITE_BUILD_ID"
    git push origin
  fi
fi
