date
./target/debug/solana-stake-o-matic --cluster mainnet-beta --markdown  $* \
   score-all \
   --score-max-commission 10 \
   --score-min-stake 100 \
   --concentration-point-discount 1500 \
   --min-avg-position 50 