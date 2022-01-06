-- :reward_fields
r.block, r.transaction_hash, to_timestamp(r.time) as timestamp, r.account, r.gateway, r.amount, r.type

-- Make sure that marker fields and fields are equivalent except for the marker
-- placeholder!
-- :reward_marker_fields
r.block, r.transaction_hash, to_timestamp(r.time) as timestamp, r.account, r.gateway, r.amount, r.type, :marker

-- :reward_list_base
select :fields
from rewards r
:scope
and r.block >= $2 and r.block < $3
order by r.block desc, :marker

-- :reward_list_rem_base
select :fields
from rewards r
:scope
and r.block = $2 and :marker> $3
order by :marker

-- :reward_block_list_base
select :fields
from rewards r 
:scope 
and r.block = $2
order by r.gateway, r.type
offset $3 fetch next $4 rows only

-- :reward_sum_hotspot_source
(select
    sum(r.amount) as amount
from reward_data r
group by r.gateway)

-- :reward_sum_time_source
(select
    sum(r.amount) as amount
from reward_data r
group by r.time)

-- :reward_sum_validator_source
(select
    sum(r.amount) as amount
from reward_data r
where r.gateway in (select address from validator_inventory)
group by r.gateway)

-- :reward_sum_base
with reward_data as (
    select
        r.amount,
        r.gateway,
        r.time
    from rewards r
    :scope
    and r.block >= $2
    and r.block <= $3
)
select
    coalesce(min(d.amount) / 100000000, 0)::float as min,
    coalesce(max(d.amount) / 100000000, 0)::float as max,
    coalesce(sum(d.amount), 0)::bigint as sum,
    coalesce(sum(d.amount) / 100000000, 0)::float as total,
    coalesce(percentile_cont(0.5) within group (order by d.amount) / 100000000, 0)::float as median,
    coalesce(avg(d.amount) / 100000000, 0)::float as avg,
    coalesce(stddev(d.amount) / 100000000, 0)::float as stddev
from :source d

-- Bucket reward_data by timestamp and gateway to be calculate statistics over hotspot totals in a bucket
-- rather than individual rewards.
-- :reward_bucketed_hotspot_source
(select
    sum(r.amount) as amount,
    r.time
from reward_data r
group by r.time, r.gateway)

-- Bucket global reward_data by timestamp only
-- :reward_bucketed_time_source
(select
    sum(r.amount) as amount,
    r.time
from reward_data r
group by r.time)

-- Bucket reward_data by timestamp and gateway to be calculate statistics over totals in a bucket
-- :reward_bucketed_validator_source
(select
    sum(r.amount) as amount,
    r.time
from reward_data r
where r.gateway in (select address from validator_inventory)
group by r.time, r.gateway)

-- :reward_bucketed_base
with time_range as (
    select
        extract(epoch from low)::bigint as low,
        extract(epoch from high)::bigint as high
    from (
        select
            timestamp as low,
            lag(timestamp) over (order by timestamp desc) as high
        from generate_series($2::timestamptz, $3::timestamptz, $4::interval) as timestamp) t
    where high is not null
),
reward_data as (
    select
        r.amount,
        r.gateway,
        r.time
    from rewards r
    :scope
    and r.time >= (select min(low) from time_range) and r.time <= (select max(high) from time_range)
)
select
    to_timestamp(t.low) as timestamp,
    coalesce(min(d.amount::float) / 100000000, 0) as min,
    coalesce(max(d.amount::float) / 100000000, 0) as max,
    coalesce(sum(d.amount), 0)::bigint as sum,
    coalesce(sum(d.amount::float) / 100000000, 0)::float as total,
    coalesce(percentile_cont(0.5) within group (order by d.amount) / 100000000, 0)::float as median,
    coalesce(avg(d.amount) / 100000000, 0)::float as avg,
    coalesce(stddev(d.amount) / 100000000, 0)::float as stddev
from time_range t
    left join :source d
    on d.time >= low and d.time < high
group by t.low
order by t.low desc;

