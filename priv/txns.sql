-- :txn_list_base
select
    t.block,
    t.time,
    t.hash,
    t.type,
    :fields
:source
:scope
:order
:limit

-- :txn_get_scope
where hash = $1

-- :txn_list_scope
where t.type = ANY($1)
and block >= $2 and block < $3

-- :txn_list_fields
t.fields

-- :txn_list_order
order by t.block desc, t.hash

-- :txn_list_limit
limit $4

-- :txn_list_source
from transactions t

-- :txn_list_rem_source
from (
    select * from transactions tr
    where tr.type = ANY($1)
    and tr.block = $2
    order by tr.hash
    ) as t

-- :txn_list_rem_scope
where t.hash > $3

-- :txn_actor_list_source
from (
    select distinct on (tr.block, tr.hash, a.actor) tr.*, a.actor
    from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash
    where tr.block >= $3 and tr.block < $4
    :actor_scope
    and tr.type = ANY($2)
    order by tr.block desc, tr.hash
    limit $5
    ) as t

-- :txn_actor_list_rem_source
from (
    select distinct on (tr.block, tr.hash, a.actor) tr.*, a.actor
    from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash
    where tr.block = $3
    :actor_scope
    and tr.type = ANY($2)
    and tr.hash > $4
    order by tr.hash
    limit $5
    ) as t

-- :txn_activity_list_fields
txn_filter_actor_activity(t.actor, t.type, t.fields) as fields

-- :txn_actor_scope
and a.actor = $1

-- :txn_hotspot_activity_actor_scope
and a.actor = $1
and a.actor_role not in ('payer', 'payee', 'owner')

-- :txn_owned_hotspot_actor_scope
and a.actor in (select address from gateway_inventory where owner = $1)

-- :txn_account_activity_actor_scope
and a.actor = $1
and a.actor_role in ('payer', 'payee', 'owner')

-- :txn_validator_activity_actor_scope
and a.actor = $1
and a.actor_role = 'validator'

-- :txn_actor_count_base
select type, count(*)
from (
    select distinct on (tr.block, tr.hash, a.actor) tr.type
    from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash
    where a.actor = $1
    :actor_scope
    and tr.type = ANY($2)) as t
group by t.type

-- :txn_location
select
    l.short_street, l.long_street,
    l.short_city, l.long_city,
    l.short_state, l.long_state,
    l.short_country, l.long_country,
    l.city_id
from locations l
where location = $1

-- :txn_hotspot_activity_min_block
select min(block) from transaction_actors
where actor = $1 and actor_role = 'gateway'

-- :txn_account_activity_min_block
select min(block) from transaction_actors
where actor = $1 and actor_role in ('payer', 'payee', 'owner')

-- :txn_oracle_activity_min_block
select min(block) from transaction_actors
where actor = $1 and actor_role = 'oracle'

-- :txn_validator_activity_min_block
select min(block) from transaction_actors
where actor = $1 and actor_role = 'validator'

-- :txn_genesis_min_block
select 1