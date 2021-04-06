-- Get block stats
-- :stats_block_times
with month_interval as (
    select to_timestamp(time) as timestamp,
           time - (lead(time) over (order by height desc)) as diff_time
    from blocks
    where to_timestamp(time) > (now() - '1 month'::interval)
),
week_interval as (
    select * from month_interval where timestamp > (now() - '1 week'::interval)
),
day_interval as (
    select * from week_interval where timestamp > (now() - '24 hour'::interval)
),
hour_interval as (
    select * from day_interval where timestamp > (now() - '1 hour'::interval)
)
select
    (select avg(diff_time) from hour_interval)::float as last_hour_avg,
    (select avg(diff_time) from day_interval)::float as last_day_avg,
    (select avg(diff_time) from week_interval)::float as last_week_avg,
    (select avg(diff_time) from month_interval)::float as last_month_avg,
    (select stddev(diff_time) from hour_interval)::float as last_hour_stddev,
    (select stddev(diff_time) from day_interval)::float as last_day_stddev,
    (select stddev(diff_time) from week_interval)::float as last_week_stddev,
    (select stddev(diff_time) from month_interval)::float as last_month_stddev


-- Get election times
-- :stats_election_times
with month_interval as (
    select to_timestamp(time) as timestamp,
           time - (lead(time) over (order by block desc)) as diff_time
    from transactions
    where to_timestamp(time) > (now() - '1 month'::interval)
    and type = 'consensus_group_v1'
),
week_interval as (
    select * from month_interval where timestamp > (now() - '1 week'::interval)
),
day_interval as (
    select * from week_interval where timestamp > (now() - '24 hour'::interval)
),
hour_interval as (
    select * from day_interval where timestamp > (now() - '1 hour'::interval)
)
select
    (select avg(diff_time) from hour_interval)::float as last_hour_avg,
    (select avg(diff_time) from day_interval)::float as last_day_avg,
    (select avg(diff_time) from week_interval)::float as last_week_avg,
    (select avg(diff_time) from month_interval)::float as last_month_avg,
    (select stddev(diff_time) from hour_interval)::float as last_hour_stddev,
    (select stddev(diff_time) from day_interval)::float as last_day_stddev,
    (select stddev(diff_time) from week_interval)::float as last_week_stddev,
    (select stddev(diff_time) from month_interval)::float as last_month_stddev

-- Get all global count stats
-- :stats_counts
select name, value from stats_inventory

-- Get currently active and last day challenge count
-- :stats_challenges
with block_poc_range as (
    select greatest(0, max(height) - coalesce((select value::bigint from vars_inventory where name = 'poc_challenge_interval'), 30)) as min,
           max(height)
    from blocks
),
block_last_day_range as (
    select min(height), max(height) from blocks
    where timestamp between now() - '24 hour'::interval and now()
),
last_day_challenges as (
    select hash from transactions
    where block between (select min from block_last_day_range) and (select max from block_last_day_range)
          and type = 'poc_receipts_v1'
)
select * from
    (select 0) as active,
    (select count(*) as last_day_challenges from last_day_challenges) as last_day

-- Get token supply
-- :stats_token_supply
select (sum(balance) / 100000000)::float as token_supply from account_inventory

-- State channel details
-- :stats_state_channels
with min as (
    select height from blocks where timestamp > (now() - '1 month'::interval) order by height limit 1
),
 month_interval as (
    select
        to_timestamp(t.time) as timestamp,
        state_channel_counts(t.type, t.fields) as counts
     from transactions t
     where t.block > (select height from min)
         and t.type = 'state_channel_close_v1'
 ),
 week_interval as (
     select * from month_interval where timestamp > (now() - '1 week'::interval)
 ),
 day_interval as (
     select * from week_interval where timestamp > (now() - '24 hour'::interval)
 )
 select
     (select sum((t.counts).num_dcs) as num_dcs from day_interval t)::bigint as last_day_dcs,
     (select sum((t.counts).num_packets) as num_dcs from day_interval t)::bigint as last_day_packets,
     (select sum((t.counts).num_dcs) as num_dcs from week_interval t)::bigint as last_week_dcs,
     (select sum((t.counts).num_packets) as num_dcs from week_interval t)::bigint as last_week_packets,
     (select sum((t.counts).num_dcs) as num_dcs from month_interval t)::bigint as last_month_dcs,
     (select sum((t.counts).num_packets) as num_dcs from month_interval t)::bigint as last_month_packets

-- Transaction fee stats
-- :stats_fees
with heights as (
    with month_start as (
        (select height from blocks where timestamp > (now() - '1 month'::interval)
         order by height limit 1)
    )
    select
        (select height as month from month_start),
        (select height as week from blocks
         where height > (select height from month_start) and timestamp > (now() - '1 week'::interval)
         order by height limit 1),
        (select height as day from blocks
         where height > (select height from month_start) and timestamp > (now() - '24 hours'::interval)
         order by height limit 1)
),
month_interval as (
    select
        t.block as block,
        sum(coalesce((t.fields->>'fee')::numeric, 0)) as fees,
        sum(coalesce((t.fields->>'staking_fee')::numeric, 0)) as staking_fees
    from transactions t
    where t.block > (select month from heights)
        and t.type not in ('poc_receipts_v1', 'poc_request_v1', 'rewards_v1', 'rewards_v2', 'consensus_group_v1')
    group by t.block
),
week_interval as (
    select * from month_interval where block > (select week from heights)
),
day_interval as (
    select * from week_interval where block > (select day from heights)
)
select
    (select sum(t.fees) from day_interval t)::bigint as last_day_txn_fees,
    (select sum(t.staking_fees) from day_interval t)::bigint as last_day_staking_fees,
    (select sum(t.fees) from week_interval t)::bigint as last_week_txn_fees,
    (select sum(t.staking_fees) from week_interval t)::bigint as last_week_staking_fees,
    (select sum(t.fees) from month_interval t)::bigint as last_month_txn_fees,
    (select sum(t.staking_fees) from month_interval t)::bigint as last_month_staking_fees;
