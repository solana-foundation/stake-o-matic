.open ./db/score-sqlite3.db

drop table avg;

create table AVG as 
select epoch,keybase_id,name,score, case when score=0 then 0 else b_score end as b_score, b_score-score as delta_score, avg_position, ap, commission, c2, epoch_credits, ec2, ec2-epoch_credits as delta_credits, 0.0 as pct, vote_address from scores A
left outer JOIN (select round( avg(epoch_credits) * (100-avg(commission))/100 * (100-avg(data_center_concentration)*4)/100 * (avg(avg_position)-49) * (avg(avg_position)-49) ) as B_score, avg(avg_position) as ap, avg(commission) as c2, avg(epoch_credits) as ec2,  vote_address as va2 from scores B 
where B.epoch between (select distinct epoch from imported)-2 and (select distinct epoch from imported)
group by vote_address)
on va2 = a.vote_address
where A.epoch = (select distinct epoch from imported)
--and score=0 and b_score>0
--and score>0 WE MUST INCLUDE ALL RECORDS - so update-scores checks all validators health
order by b_score desc
;

-- compute PCT (informative)
update avg as U
set pct = B_score / (select sum(A.b_score) from avg A where A.epoch = U.epoch) * 100
;

-- show validators with pct assgined (informative)
select * from AVG 
order by pct desc
where pct>0;

.exit
