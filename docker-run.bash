#!/usr/bin/env bash
set -x

SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
DB_PATH="$SCRIPT_DIR/db"
SQLITE_SCORES_PATH="$DB_PATH/score-sqlite3.db"
HISTORIC_DATA="https://github.com/marinade-finance/staking-status/raw/main/scores.sqlite3"

mkdir -p "$DB_PATH"
wget "$HISTORIC_DATA" -O "$SQLITE_SCORES_PATH"

docker run \
  --name stake-o-matic \
  --user "$UID" \
  --rm \
  --volume "$DB_PATH:/usr/local/db" \
  --env "VALIDATORS_APP_TOKEN=$VALIDATORS_APP_TOKEN" \
  stake-o-matic ./clean-score-all-mainnet.bash
