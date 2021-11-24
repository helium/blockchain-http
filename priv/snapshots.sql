-- :snapshot_list_base
select 
    b.height, b.snapshot_hash 
from blocks b
where b.snapshot_hash is not null and b.snapshot_hash != ''
:scope
order by height desc
:limit

-- :snapshot_list_before_scope
and b.height < $1