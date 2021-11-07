--
-- CHECK FOR commision changes
--
drop if exists table t1;
create table t1 as
select vote_address,commission from scores where epoch=243
EXCEPT select vote_address,commission from scores where epoch=242
;
select * from scores where vote_address in ( select vote_address from t1 )
order by vote_address,epoch
;
