./target/debug/solana-stake-o-matic --markdown  $* \
   --min-epoch-credit-percentage-of-average 20 \
   score-all \
   --score-max-commission 10 \
   --score-min-stake 100 \
   --commission-point-discount 5000 \
   --concentration-point-discount 1000
   
# --cluster mainnet-beta