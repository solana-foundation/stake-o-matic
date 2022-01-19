# stake-o-matic

## Build with cargo and run
```bash
cargo build
wget "https://github.com/marinade-finance/staking-status/raw/main/scores.sqlite3" -O "db/score-sqlite3.db"
./clean-score-all-mainnet.bash
```

## Build with docker and run
```bash
./docker-build.bash
./docker-run.bash
```
