-- Get currently active and last day challenge count
-- :challenges_stats
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
          and type in ('poc_receipts_v1', 'poc_receipts_v2')
)
select * from
    (select 0) as active,
    (select count(*) as last_day_challenges from last_day_challenges) as last_day

