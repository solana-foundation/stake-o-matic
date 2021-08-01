./target/debug/solana-stake-o-matic --cluster mainnet-beta --markdown  $* \
   --min-epoch-credit-percentage-of-average 20 \
   score-all \
   --score-max-commission 8 \
   --score-min-stake 100 \
   --concentration-point-discount 1500
   
# --cluster mainnet-beta