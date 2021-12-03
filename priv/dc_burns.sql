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
select $2, t.type, sum(t.amount)::bigint from (
   select sum(d.amount) as amount, max(d.time) as time, d.type, d.block
   from dc_burns d 
   where d.block > $1
   group by d.block, d.type
) t group by t.type;


-- :burn_sum
select d.type, sum(d.amount)::bigint as amount
from dc_burns d 
where d.time >= extract(epoch from $1::timestamptz)
    and d.time <= extract(epoch from $2::timestamptz)
group by d.type;

-- :burn_bucketed_sum
with time_range as (
    select
        extract(epoch from low)::bigint as low,
        extract(epoch from high)::bigint as high
    from (
        select
            timestamp as low,
            lag(timestamp) over (order by timestamp desc) as high
        from generate_series($1::timestamptz, $2::timestamptz, $3::interval) as timestamp) t
    where high is not null
),
burn_data as (
    select sum(d.amount)::bigint as amount, d.time, d.type
    from dc_burns d 
    where d.time >= (select min(low) from time_range) 
        and d.time <= (select max(high) from time_range)
    group by d.time, d.type    
)
select 
    t.low, 
    d.type, 
    sum(d.amount)::bigint 
from time_range t
    left join burn_data d
    on  d.time >= low and d.time < high
group by t.low, d.type
order by t.low desc;
