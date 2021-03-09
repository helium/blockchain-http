-- :validator_list_base
select
    (select max(height) from blocks) as height,
    l.address,
    l.owner,
    l.stake,
    l.status,
    l.last_heartbeat,
    l.version_heartbeat,
    l.nonce,
    l.first_block,
    s.online as online_status,
    s.block as block_status,
    s.listen_addrs as listen_addrs,
    s.release_version as release_version
from validator_inventory l
left join validator_status s on s.address = l.address
:scope
:order
:limit

-- :validator_list_order
order by l.first_block desc, l.address

-- :validator_list_before_scope
where (l.address > $1 and l.first_block = $2) or (l.first_block < $2)

-- :owner_validator_list_scope
where l.owner = $1

-- :owner_validator_list_before_scope
where l.owner = $1
    and ((l.address > $2 and l.first_block = $3) or (l.first_block < $3))


-- :validator_elected_list
with field_members as (
    select fields->'members' as members
    from transactions
    where type = 'consensus_group_v1' :filter
    order by block desc
    limit 1
),
members as (
    select *
    from jsonb_array_elements_text((select members from field_members))
)
:validator_select

-- :validator_elected_list_scope
where l.address in (select * from members)

-- Validator stats
-- :validator_stats
select v.status, count(*), (sum(stake) / 100000000)::float
from validator_inventory v
group by v.status


-- :validator_active
select * from stats_inventory where name = 'validators'