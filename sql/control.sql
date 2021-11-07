.open db/score-sqlite3.db
.mode column
.headers ON
select epoch,rank,keybase_id,name, round(pct,4) as pct, avg_score, ROUND(mult,4) as mult,
   round(avg_pos,4) as avg_pos,
   epoch_credits,avg_ec,delta_credits,
   avg_commiss,round(dcc2,5) as dcc2 from AVG 
where pct>0 
order by rank
LIMIT 20
;
select count(*) as validators_with_pct from avg where pct<>0;
.exit
