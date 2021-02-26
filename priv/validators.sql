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
    :extend
from validator_inventory l
:scope
:order
:limit

-- :validator_list_order
order by l.first_block desc, l.address

-- :validator_list_before_scope
where (l.address > $1 and l.first_block = $2) or (l.first_block < $2)

-- :validator_speculative_extend
, (select greatest(l.nonce, coalesce(max(p.nonce), l.nonce))
    from pending_transactions p
    where p.address = l.address and nonce_type='validator' and status != 'failed'
) as speculative_nonce
