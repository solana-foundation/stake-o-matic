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

BIN=solana-stake-o-matic
BIN_TARGET=$BIN-$TARGET

if [[ $1 = master ]]; then
  URL=https://github.com/solana-labs/stake-o-matic/raw/master-bin/$BIN_TARGET
elif [[ -n $1 ]]; then
  URL=https://github.com/solana-labs/stake-o-matic/releases/download/$1/$BIN_TARGET
else
  URL=https://github.com/solana-labs/stake-o-matic/releases/latest/download/$BIN_TARGET
fi

set -x
curl -fL $URL -o $BIN
chmod +x $BIN
ls -l $BIN
./$BIN --version
