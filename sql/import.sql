.open ./db/score-sqlite3.db

-- create table to receive stake-o-matic data
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

-- import stake-o-matic data
.mode csv
.import ./db/score-all-mainnet-beta/mainnet-beta-validator-detail.csv imported
--remove header row
delete FROM imported where identity='identity';

--add pct and stake-concentration columns 
ALTER table imported add pct FLOAT;
ALTER table imported add stake_conc FLOAT;
ALTER table imported add adj_credits INTEGER;
UPDATE imported set 
     pct = round(score * 100.0 / (select sum(score) from imported),4),
     stake_conc = round(active_stake * 100.0 / (select sum(active_stake) from imported),4),
     adj_credits = CAST((epoch_credits * (100-commission-3*data_center_concentration)/100) as INTEGER)
   ;

--recompute avg_position based on adj_credits
update imported
set avg_position = adj_credits * 50 / (select avg(adj_credits) from scores B where adj_credits>30000);


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

-- add imported epoch to table scores
create TABLE if not EXISTS scores as select * from imported;
DELETE FROM scores where epoch = (select DISTINCT epoch from imported);
INSERT INTO scores select * from imported;


-- recompute avg table with last 5 epochs
-- if score=0 from imported => below nakamoto coefficient, or commission 100% or less than 100 SOL staked
-- also we set score=0 if below 50% avg or less than 5 epochs on record
-- create pct column and set to zero, will update after when selecting top 200
DROP TABLE IF EXISTS avg;
create table AVG as 
select 0 as rank, epoch,keybase_id, vote_address,name,
   case when score=0 or mult<=0 or score_records<5 then 0 else ROUND(base_score*mult) end as avg_score, 
   base_score, ap-49 mult, ap as avg_pos, commission, round(c2,2) as avg_commiss, dcc2,
   epoch_credits, cast(ec2 as integer) as avg_ec, epoch_credits-ec2 as delta_credits,
   0.0 as pct, score_records, avg_active_stake
from imported A
left outer JOIN (
       select count(*) as score_records,
            round( avg(b.adj_credits) ) as base_score, 
            avg(b.avg_position) as ap, avg(b.avg_position)-49 as mult, avg(b.commission) as c2, ROUND(avg(b.epoch_credits)) as ec2,
            avg(b.data_center_concentration) as dcc2, b.vote_address as va2, avg(b.active_stake) as avg_active_stake
       from scores B 
       where B.epoch between (select distinct epoch from imported)-4 and (select distinct epoch from imported)
       group by vote_address
       )
     on va2 = a.vote_address
where A.epoch = (select distinct epoch from imported)
--and score>0 NOTE: WE MUST INCLUDE ALL RECORDS - so update-scores checks all validators' health
order by base_score desc
;

-- compute rank
drop table if exists temp;
create table temp as select vote_address, RANK() over (order by avg_score DESC) as rank from avg;
-- set rank in avg table
update avg 
set rank = (select rank from temp where temp.vote_address=avg.vote_address);

-- SELECT TOP 200
drop table if exists temp;
create table temp as select * from avg order by avg_score desc LIMIT 200;
-- set pct ONLY ON TOP 200
update avg as U
set pct = avg_score / (select sum(A.avg_score) from temp A where A.epoch = U.epoch) * 100
where exists (select 1 from temp A where A.vote_address = U.vote_address)
;

-- show top validators with pct assgined (informative)
.mode column
.headers ON
select epoch,rank,keybase_id,name, round(pct,4) as pct, avg_score, ROUND(mult,4) as mult,
   round(avg_pos,4) as avg_pos,
   epoch_credits,avg_ec,delta_credits,
   avg_commiss,round(dcc2,5) as dcc2 from AVG 
where pct>0 
order by rank
LIMIT 15
;
select count(*) as validators_with_pct from avg where pct<>0;
.exit
