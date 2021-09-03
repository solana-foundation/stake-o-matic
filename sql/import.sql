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

-- recompute avg last 3 epochs
-- * (avg(b.avg_position)-49) * (avg(b.avg_position)-49) ) 
DROP TABLE IF EXISTS avg;
create table AVG as 
select epoch,keybase_id,name,score, 
   case when score=0 or mult<=0 then 0 else b_score*mult*mult end as b_score, 
   b_score-score as delta_score, ap-49 mult, avg_position, ap, commission, c2, dcc2,
   epoch_credits, ec2, epoch_credits-ec2 as delta_credits, 
   0.0 as pct, vote_address 
from scores A
left outer JOIN (
       select round( avg(b.epoch_credits) * (100-avg(b.commission))/100 * (100-avg(b.data_center_concentration)*3)/100 ) as B_score, 
              avg(b.avg_position) as ap, avg(b.avg_position)-49 as mult, avg(b.commission) as c2, avg(b.epoch_credits) as ec2, avg(b.data_center_concentration) as dcc2, b.vote_address as va2 
       from scores B 
       where B.epoch between (select distinct epoch from imported)-2 and (select distinct epoch from imported)
       group by vote_address
       )
     on va2 = a.vote_address
where A.epoch = (select distinct epoch from imported)
--and score=0 and b_score>0
--and score>0 WE MUST INCLUDE ALL RECORDS - so update-scores checks all validators health
order by b_score desc
;

-- compute PCT (informative)
-- SELECTING TOP 200
drop table if exists temp;
create table temp as select * from avg order by b_score desc LIMIT 200;
update avg as U
set pct = B_score / (select sum(A.b_score) from temp A where A.epoch = U.epoch) * 100
where exists (select 1 from temp A where A.vote_address = U.vote_address)
;

-- show top validators with pct assgined (informative)
.mode column
.headers ON
select epoch,keybase_id,name, round(pct,2) as pct, b_score,delta_score,avg_position,ec2, round(c2) as comm,round(dcc2) as dcc2, vote_address from AVG 
where pct>0 
order by pct desc
LIMIT 10
;

.exit
