#!/bin/bash

set -eux

echo "steps:"

group=1
while IFS=, read -r RESERVE_ACCOUNT_ADDRESS STAKE_AUTHORITY_KEYPAIR; do
  echo "  - command: \"./stake-o-matic.sh\""
  echo "    label: \"Group ${group}\""
  echo "    agents:"
  echo "    - \"queue=${AGENT_QUEUE:?}\""
  echo "    env:"
  echo "      RESERVE_ACCOUNT_ADDRESS: \"${RESERVE_ACCOUNT_ADDRESS}\""
  echo "      STAKE_AUTHORITY_KEYPAIR: \"${STAKE_AUTHORITY_KEYPAIR}\""
  echo "    timeout: 120"
  group=$(( group + 1 ))
done<"${STAKE_SOURCE_CSV:?}"
