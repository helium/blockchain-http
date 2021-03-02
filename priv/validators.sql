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
    l.first_block
from validator_inventory l
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

