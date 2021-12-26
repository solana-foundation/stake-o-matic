insert into scores
select 
epoch	,
null as keybase_id	,
name	,
null as identity	,
vote_address	,
score	,
average_position as avg_position	,
commission	,
avg_active_stake as active_stake	,
this_epoch_credits epoch_credits	,
data_center_concentration	,
null as can_halt_the_network_group	,
null as stake_state	,
null as stake_state_reason	,
null as www_url	,
pct	,
null as stake_conc	,
null as adj_credits	
from scores2