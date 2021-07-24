./target/debug/solana-stake-o-matic --markdown  $* \
   --min-epoch-credit-percentage-of-average 0 \
   score-all \
   --score-max-commission 10 \
   --commission-point-discount 2000 \
   --concentration-point-discount 2000
   
# --cluster mainnet-beta