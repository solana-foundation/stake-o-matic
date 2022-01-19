#!/bin/bash
set -ex
rm -rf db/score-all-mainnet-beta
. ./score-all-mainnet.sh
. ./import-into-sqlite.sh
