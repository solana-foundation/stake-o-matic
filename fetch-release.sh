#!/usr/bin/env bash

set -e

case "$(uname)" in
Linux)
  TARGET=x86_64-unknown-linux-gnu
  ;;
Darwin)
  TARGET=x86_64-apple-darwin
  ;;
*)
  echo "machine architecture is currently unsupported"
  exit 1
  ;;
esac

if [[ $1 = master ]]; then
  RELEASE_BINARY=https://github.com/solana-labs/stake-o-matic/raw/master-bin/sys-$TARGET
elif [[ -n $1 ]]; then
  RELEASE_BINARY=https://github.com/solana-labs/stake-o-matic/releases/download/$1/solana-stake-o-matic-$TARGET
else
  RELEASE_BINARY=https://github.com/solana-labs/stake-o-matic/releases/latest/download/solana-stake-o-matic-$TARGET
fi

set -x
curl -sSfL $RELEASE_BINARY -o solana-stake-o-matic
chmod +x solana-stake-o-matic
ls -l solana-stake-o-matic
./solana-stake-o-matic --version
