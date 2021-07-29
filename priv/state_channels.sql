-- Get currently active and last day state_channel count
-- :state_channels_stats
with block_last_day_range as (
    select min(height), max(height) from blocks
    where timestamp between now() - '24 hour'::interval and now()
),
last_day_state_channels as (
    select hash from transactions
    where block between (select min from block_last_day_range) and (select max from block_last_day_range)
          and type = 'state_channel_close_v1'
)
select * from
    (select count(*) as last_day_state_channels from last_day_state_channels) as last_day

