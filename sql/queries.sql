-- * --
select pct, A.*
from mainnet as A
where score>0
order by pct desc
-- * --
-- compare epochs
select epoch,keybase_id,name,score,b_score, avg_position, ap, commission, c2, epoch_credits, ec2, can_halt_the_network_group as h1, h2 from scores A
JOIN (select score as B_score, avg_position as ap, commission as c2, epoch_credits as ec2, can_halt_the_network_group as h2, vote_address as va2 from scores B
where B.epoch = 214)
on va2 = a.vote_address
where A.epoch = 213
--and score=0 and b_score>0
order by score desc
-- * --