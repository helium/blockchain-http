-- :reward_block_range
with max as (
     select height from blocks where timestamp < $1 order by height desc limit 1
),
min as (
    select height from blocks where timestamp >= $2 order by height limit 1
)
select (select height from max) as max, (select height from min) as min

-- :reward_list_base
select :fields
from rewards r
:scope
and r.block >= $2 and r.block <= $3
order by r.block desc, r.transaction_hash

-- :reward_list_rem_base
select :fields
from rewards r
:scope
and r.block = $2 and r.transaction_hash > $3
order by r.transaction_hash

-- :reward_sum_base
select
    coalesce(min(r.amount) / 100000000, 0)::float as min,
    coalesce(max(r.amount) / 100000000, 0)::float as max,
    coalesce(sum(r.amount), 0)::bigint as sum,
    coalesce(sum(r.amount) / 100000000, 0)::float as total,
    coalesce(percentile_cont(0.5) within group (order by r.amount) / 100000000, 0)::float as median,
    coalesce(avg(r.amount) / 100000000, 0)::float as avg,
    coalesce(stddev(r.amount) / 100000000, 0)::float as stddev
from rewards r
:scope
and r.block >= $2 and r.block <= $3

-- :reward_stats_base
with time_range as (
    select generate_series(date_trunc($4::text, $2::timestamptz), date_trunc($4::text, $3::timestamptz), $5) as timestamp
),
max as (
    select height from blocks where timestamp < (select max(timestamp) from time_range) order by height desc limit 1
),
min as (
    select height from blocks where timestamp >= (select min(timestamp) from time_range) order by height limit 1
),
data as (
    select
        min(r.amount) as min,
        max(r.amount) as max,
        sum(r.amount) as sum,
        percentile_cont(0.5) within group (order by r.amount) as median,
        avg(r.amount) as avg,
        stddev(r.amount) as stddev,
        date_trunc($4::text, to_timestamp(r.time)) as timestamp
    from rewards r
    :scope
    and r.block >= (select height from min) and r.block <= (select height from max)
    group by date_trunc($4::text, to_timestamp(r.time))
)
select
    t.timestamp,
    coalesce(d.min / 100000000, 0)::float as min,
    coalesce(d.max / 100000000, 0)::float as max,
    coalesce(d.sum, 0)::bigint as sum,
    coalesce(d.sum / 100000000, 0)::float as total,
    coalesce(d.median / 100000000, 0)::float as median,
    coalesce(d.avg / 100000000, 0)::float as avg,
    coalesce(d.stddev / 100000000, 0)::float as stddev
from time_range t left join data d on t.timestamp = d.timestamp
where t.timestamp < (select max(timestamp) from time_range)
order by t.timestamp desc;
