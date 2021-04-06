-- Get all global count stats
-- :stats_counts
select name, value from stats_inventory

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
