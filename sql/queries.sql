-- SQLite3
/*SELECT identity, ` stake_state`, ` score`, ` commission`, ` active_stake`, ` epoch_credits`
, ` stake_state_reason`
FROM mainnet
order by ` epoch_credits` desc;
*/
--.schema data

/*CREATE TABLE mainnet(
  identity TEXT, 
  score INTEGER, 
  commission SHORT, 
  active_stake INTEGER, 
  epoch_credits INTEGER,
  stake_state TEXT, 
  stake_state_reason TEXT
)
*/
--insert into data 
--select * from mainnet
/*select identity,
  score, 
  commission, 
  active_stake/1e9, 
  epoch_credits
  --max(epoch_credits),
  --avg(epoch_credits)
 from mainnet
order by active_stake desc;
*/

--select sum(active_stake) from mainnet where active_stake is not null;
/*
select 'below half avg epoch_credits',count(*),
        "stake",sum(active_stake)/1e9
   from mainnet
   where epoch_credits < (select avg(epoch_credits)*0.50 from mainnet)
*/

-- if epoch_credits < 60% of max epoch_credits, discard 
select *
   from mainnet
   where epoch_credits < (select max(epoch_credits)*0.60 from mainnet)
   order by epoch_credits desc
   