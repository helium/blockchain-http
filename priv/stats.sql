-- Get all global count stats
-- :stats_counts
select name, value from stats_inventory

-- Get token supply
-- :stats_token_supply
select
    coalesce((select (sum(stake) / 100000000)::float from validator_inventory), 0)
    + coalesce((select (sum(balance) / 100000000)::float from account_inventory), 0)

