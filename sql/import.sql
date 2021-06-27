.open ./db/data-mainnet-beta/sqlite3.db
DROP TABLE IF EXISTS mainnet;
CREATE TABLE mainnet(
  identity TEXT, 
  score INTEGER, 
  commission SHORT, 
  active_stake INTEGER, 
  epoch_credits INTEGER,
  data_center_concentration DOUBLE,
  can_halt_the_network_group BOOL,
  low_credits BOOL,
  insufficient_self_stake BOOL,
  stake_state TEXT, 
  stake_state_reason TEXT
);
.mode csv
.import ./db/data-mainnet-beta/validator-detail.csv mainnet
--remove header row
delete FROM mainnet where identity='identity';
--add pct column 
ALTER table mainnet add pct FLOAT;
UPDATE mainnet set pct = round(score * 100.0 / (select sum(score) from mainnet),4);
--control, show total staked
select 'validators',count(*),'total staked',sum(active_stake) from mainnet;
select 'avg epoch_credits',avg(epoch_credits) from mainnet;
select 'below half avg epoch_credits',count(*),
        "stake",sum(active_stake)
   from mainnet
   where epoch_credits < (select avg(epoch_credits)/2 from mainnet)
   ;
.exit
