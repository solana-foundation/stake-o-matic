
select 
261 as epoch	,
max(keybase_id)	,
max(name),
max(identity)	,
vote_address,
avg(score),
avg(avg_position),
avg(commission),
avg(active_stake),
CAST(avg(epoch_credits) as INTEGER),
avg(data_center_concentration),
min(can_halt_the_network_group),
min(stake_state),
min(stake_state_reason),
max(www_url),
avg(pct),
avg(stake_conc),
CAST(avg(adj_credits) as INTEGER)
from scores
where epoch BETWEEN 260 and 262
group by vote_address
