.open ./db/data-mainnet-beta/sqlite3.db
CREATE TABLE IF NOT EXISTS mainnet(
  identity TEXT, 
  score INTEGER, 
  commission SHORT, 
  active_stake INTEGER, 
  epoch_credits INTEGER,
  stake_state TEXT, 
  stake_state_reason TEXT
);
delete from mainnet;
.mode csv
.import ./db/data-mainnet-beta/validator-detail.csv mainnet
--remove header row
delete FROM mainnet where identity='identity';
--show total stake
select 'validators',count(*),'total staked',sum(active_stake)/1e9 from mainnet;
select 'avg epoch_credits',avg(epoch_credits) from mainnet;
select 'below half avg epoch_credits',count(*),
        "stake",sum(active_stake)/1e9
   where epoch_credits < (select avg(epoch_credits)/2 from mainnet)
   from mainnet;
.exit

