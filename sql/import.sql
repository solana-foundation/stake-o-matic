.open ./db/score-all-mainnet-beta/sqlite3.db
DROP TABLE IF EXISTS mainnet;
CREATE TABLE mainnet(
  identity TEXT, 
  vote_address TEXT, 
  score INTEGER, 
  avg_position INTEGER, 
  commission SHORT, 
  active_stake INTEGER, 
  epoch_credits INTEGER,
  data_center_concentration DOUBLE,
  can_halt_the_network_group BOOL,
  stake_state TEXT, 
  stake_state_reason TEXT
);
.mode csv
.import ./db/score-all-mainnet-beta/mainnet-beta-validator-detail.csv mainnet
--.import ./db/score-all-testnet/testnet-validator-detail.csv mainnet
--remove header row
delete FROM mainnet where identity='identity';
--add pct column 
ALTER table mainnet add pct FLOAT;
UPDATE mainnet set pct = round(score * 100.0 / (select sum(score) from mainnet),4);
--control, show total staked
select 'validators',count(*),'total staked',sum(active_stake) from mainnet;
select 'validators with 0 score count:',count(*),
        "sum stake",sum(active_stake)
   from mainnet
   where pct=0
   ;
select 'validators with non-zero score count:',count(*),
        "sum stake",sum(active_stake)
   from mainnet
   where pct>0
   ;
select 'avg epoch_credits',avg(epoch_credits),
      'max epoch credits',max(epoch_credits),
      'min epoch credits',min(epoch_credits), min(epoch_credits)/avg(epoch_credits)*100, "% of avg",
      char(10) || 'max score',max(score),
      'min score',min(score),
      char(10) || 'max pct',max(pct),
      'min pct',min(pct)
 from mainnet
 where pct>0;
.exit
