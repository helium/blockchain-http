-- :burn_list_base
select d.block, d.actor, d.type, d.amount, d.oracle_price
from dc_burns d 
:scope
order by d.block desc, d.actor
:limit

-- :burn_list_scope
where d.type = ANY($1)

-- :burn_list_before_scope
where d.type = ANY($1)
and (d.actor > $2 and d.block = $3) or (d.block < $3)

-- :burn_stats
with month_interval as (
   select sum(d.amount) as amount, d.time, d.type
   from dc_burns d 
   where d.time > extract(epoch from (now() - '1 month'::interval))
   group by d.time, d.type
),
week_interval as (
    select * from month_interval 
    where time > extract(epoch from (now() - '1 week'::interval))
),
day_interval as (
    select * from week_interval 
    where time > extract(epoch from (now() - '24 hour'::interval))
)
select 'last_day', t.type, sum(t.amount)::bigint from day_interval t group by t.type
union
select 'last_week', t.type, sum(t.amount)::bigint from week_interval t group by t.type
union
select 'last_month', t.type, sum(t.amount)::bigint from month_interval t group by t.type;