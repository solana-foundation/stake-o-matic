.open ./db/score-sqlite3.db
DROP TABLE IF EXISTS imported;
CREATE TABLE imported(
  epoch INT,
  keybase_id TEXT,
  name TEXT,
  identity TEXT, 
  vote_address TEXT, 
  score INTEGER, 
  avg_position REAL, 
  commission SHORT, 
  active_stake INTEGER, 
  epoch_credits INTEGER,
  data_center_concentration DOUBLE,
  can_halt_the_network_group BOOL,
  stake_state TEXT, 
  stake_state_reason TEXT,
  www_url TEXT
);
.mode csv
.import ./db/score-all-mainnet-beta/mainnet-beta-validator-detail.csv imported
--.import ./db/score-all-testnet/testnet-validator-detail.csv imported
--remove header row
delete FROM imported where identity='identity';
--add pct column 
ALTER table imported add pct FLOAT;
ALTER table imported add stake_conc FLOAT;
UPDATE imported set 
     pct = round(score * 100.0 / (select sum(score) from imported),4),
     stake_conc = round(active_stake * 100.0 / (select sum(active_stake) from imported),4)
   ;
--control, show total staked
select DISTINCT epoch from imported;
select 'validators',count(*),'total staked',sum(active_stake) from imported;
select 'validators with 0 score count:',count(*),
        "sum stake",sum(active_stake)
   from imported
   where pct=0
   ;
select 'validators with non-zero score count:',count(*),
        "sum stake",sum(active_stake)
   from imported
   where pct>0
   ;
select 'avg epoch_credits',avg(epoch_credits),
      'max epoch credits',max(epoch_credits),
      'min epoch credits',min(epoch_credits), min(epoch_credits)/avg(epoch_credits)*100, "% of avg",
      char(10) || 'max score',max(score),
      'min score',min(score),
      char(10) || 'max pct',max(pct),
      'min pct',min(pct)
 from imported
 where pct>0;
-- add to scores --drop table scores;
create TABLE if not EXISTS scores as select * from imported;
DELETE FROM scores where epoch = (select DISTINCT epoch from imported);
INSERT INTO scores select * from imported;
.exit
