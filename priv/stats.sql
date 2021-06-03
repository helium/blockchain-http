-- Get all global count stats
-- :stats_counts
select name, value from stats_inventory

-- Get token supply
-- :stats_token_supply
select (sum(balance) / 100000000)::float as token_supply from account_inventory

