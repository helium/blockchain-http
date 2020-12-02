-- :oracle_price_list_base
select p.block, p.price, b.timestamp
from oracle_prices p inner join blocks b on p.block = b.height
:scope
order by p.block desc limit :limit

-- :oracle_price_predictions
select time, price 
from oracle_price_predictions 
order by time DESC
