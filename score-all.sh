./target/debug/solana-stake-o-matic --markdown  $* \
   --min-epoch-credit-percentage-of-average 0 \
   score-all \
   --score-max-commission 10 \
   --commission-point-discount 15000
   
# --cluster mainnet-beta