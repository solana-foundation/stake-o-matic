#!/usr/bin/env bash
#
# Downloads and runs the latest stake-o-matic binary
#
set -ex

DIR="$(dirname "$0")"

# Convert space-delimited string into array
IFS=" " read -r -a MAINNET_BETA_JSON_RPC_URL <<< "$MAINNET_BETA_JSON_RPC_URL"
IFS=" " read -r -a TESTNET_JSON_RPC_URL <<< "$TESTNET_JSON_RPC_URL"

"$DIR"/fetch-release.sh "$STAKE_O_MATIC_RELEASE"

if [[ -n $FOLLOWER ]]; then
  REQUIRE_CLASSIFICATION="--require-classification"
else
  CSV_OUTPUT_MODE="--csv-output-mode first"
  EPOCH_CLASSIFICATION="--epoch-classification first"
fi

# Re-classifies all validators, even if they have already been classified
if [[ -n $IGNORE_EXISTING_CLASSIFICATION ]]; then
  IGNORE_EXISTING_CLASSIFICATION="--ignore-existing-classification"
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

if [[ -n $MAX_POOR_BLOCK_PRODUCER_PERCENTAGE ]]; then
  MAX_POOR_BLOCK_PRODUCER_PERCENTAGE="--max-poor-block-producer-percentage $MAX_POOR_BLOCK_PRODUCER_PERCENTAGE"
fi

if [[ -n $REQUIRE_DRY_RUN_TO_DISTRIBUTE_STAKE ]]; then
  REQUIRE_DRY_RUN_TO_DISTRIBUTE_STAKE="--require-dry-run-to-distribute-stake"
fi

if [[ -n $MIN_RELEASE_VERSION ]]; then
  MIN_RELEASE_VERSION="--min-release-version $MIN_RELEASE_VERSION"
fi

if [[ -n $MAX_RELEASE_VERSION ]]; then
  MAX_RELEASE_VERSION="--max-release-version $MAX_RELEASE_VERSION"
fi

if [[ -n $MAX_POOR_VOTER_PERCENTAGE ]]; then
  MAX_POOR_VOTER_PERCENTAGE="--max-poor-voter-percentage $MAX_POOR_VOTER_PERCENTAGE"
fi

if [[ -n $MIN_EPOCH_CREDIT_PERCENTAGE_OF_AVERAGE ]]; then
  MIN_EPOCH_CREDIT_PERCENTAGE_OF_AVERAGE="--min-epoch-credit-percentage-of-average $MIN_EPOCH_CREDIT_PERCENTAGE_OF_AVERAGE"
fi

if [[ -n $PERFORMANCE_WAIVER_RELEASE_VERSION ]]; then
  PERFORMANCE_WAIVER_RELEASE_VERSION="--performance-waiver-release-version $PERFORMANCE_WAIVER_RELEASE_VERSION"
fi

if [[ -n $USE_RPC_TX_SUBMISSION ]]; then
  USE_RPC_TX_SUBMISSION="--use-rpc-tx-submission $USE_RPC_TX_SUBMISSION"
fi

if [[ -n $IGNORE_STAKE_DISTRIBUTION_ERRORS ]]; then
  IGNORE_STAKE_DISTRIBUTION_ERRORS="--ignore-stake-distribution-errors"
fi

# shellcheck disable=SC2206
TESTNET_ARGS=(
  --cluster testnet
  --quality-block-producer-percentage 30
  --max-infrastructure-concentration 25
  --infrastructure-concentration-affects destake-new
  --max-old-release-version-percentage 20
  --performance-db-url ${PERFORMANCE_DB_URL:?}
  --performance-db-token ${PERFORMANCE_DB_TOKEN:?}
#  --require-performance-metrics-reporting
)

# shellcheck disable=SC2206
MAINNET_BETA_ARGS=(
  --cluster mainnet-beta
  --quality-block-producer-percentage 30
  --max-active-stake 3000000
  --max-commission 10
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
#  --require-performance-metrics-reporting
)

# shellcheck disable=SC2206
NOT_A_STAKE_POOL_ARGS=(
  stake-pool-v0
  --min-reserve-stake-balance ${MIN_RESERVE_STAKE_BALANCE:?}
  ${RESERVE_ACCOUNT_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
  ${BASELINE_STAKE_AMOUNT:?}
  $IGNORE_STAKE_DISTRIBUTION_ERRORS
)

# shellcheck disable=SC2206
STAKE_POOL_ARGS=(
  --db-suffix stake-pool
  stake-pool
  ${STAKE_POOL_ADDRESS:?}
  ${STAKE_AUTHORITY_KEYPAIR:?}
  ${BASELINE_STAKE_AMOUNT:?}
)

# shellcheck disable=SC2206
SHARED_ARGS=(
  --mainnet-beta-json-rpc-url ${MAINNET_BETA_JSON_RPC_URL[@]:?}
  --testnet-json-rpc-url ${TESTNET_JSON_RPC_URL[@]:?}
  $USE_RPC_TX_SUBMISSION
  $MAX_POOR_BLOCK_PRODUCER_PERCENTAGE
  $REQUIRE_DRY_RUN_TO_DISTRIBUTE_STAKE
  $BLOCKLIST
  $CSV_OUTPUT_MODE
  $EPOCH_CLASSIFICATION
  $CONFIRM
  $REQUIRE_CLASSIFICATION
  $MIN_RELEASE_VERSION
  $MAX_RELEASE_VERSION
  $PERFORMANCE_WAIVER_RELEASE_VERSION
  $MAX_POOR_VOTER_PERCENTAGE
  $MIN_EPOCH_CREDIT_PERCENTAGE_OF_AVERAGE
  $IGNORE_EXISTING_CLASSIFICATION
)

if [[ $CLUSTER = "testnet-stake-pool" ]]; then
  ./solana-stake-o-matic "${TESTNET_ARGS[@]}" "${SHARED_ARGS[@]}" "${STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER = "mainnet-beta-stake-pool" ]]; then
  ./solana-stake-o-matic "${MAINNET_BETA_ARGS[@]}" "${SHARED_ARGS[@]}" "${STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER == "testnet" ]]; then
  ./solana-stake-o-matic "${TESTNET_ARGS[@]}" "${SHARED_ARGS[@]}" "${NOT_A_STAKE_POOL_ARGS[@]}"
elif [[ $CLUSTER == "mainnet-beta" ]]; then
  ./solana-stake-o-matic "${MAINNET_BETA_ARGS[@]}" "${SHARED_ARGS[@]}" "${NOT_A_STAKE_POOL_ARGS[@]}"
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
