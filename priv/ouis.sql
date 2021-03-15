-- :oui_list_base
select
    (select max(height) from blocks) as height,
    l.oui,
    l.owner,
    l.nonce,
    l.addresses,
    l.subnets,
    l.first_block
from oui_inventory l
:scope
:order
:limit

-- :oui_list_order
order by l.first_block desc, l.oui

-- :oui_list_before_scope
where (l.oui > $1 and l.first_block = $2) or (l.first_block < $2)

-- :owner_oui_list_scope
where l.owner = $1

-- :owner_oui_list_before_scope
where l.owner = $1
    and ((l.oui > $2 and l.first_block = $3) or (l.first_block < $3))

-- :oui_active
select count(*) from oui_inventory;
