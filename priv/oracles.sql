-- :oracle_price_list_base
select p.block, p.price, b.timestamp
from oracle_prices p inner join blocks b on p.block = b.height
:scope
order by p.block desc limit :limit

-- :oracle_price_predictions
select time, price 
from oracle_price_predictions 
order by time DESC

-- :oracle_price_stats
with price_data as (
    select
        p.price
    from oracle_prices p inner join blocks b on p.block = b.height
    where b.time >= extract(epoch from $1::timestamptz)
    and b.time <= extract(epoch from $2::timestamptz)
)
select
    coalesce(min(d.price) / 100000000, 0)::float as min,
    coalesce(max(d.price) / 100000000, 0)::float as max,
    coalesce(percentile_cont(0.5) within group (order by d.price) / 100000000, 0)::float as median,
    coalesce(avg(d.price) / 100000000, 0)::float as avg,
    coalesce(stddev(d.price) / 100000000, 0)::float as stddev
from price_data d