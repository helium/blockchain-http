-- Get block stats
-- :block_times
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
