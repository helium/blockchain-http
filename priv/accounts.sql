-- :account_list_base
select 
    (select max(height) from blocks) as height, 
    l.address, 
    l.dc_balance, 
    l.dc_nonce, 
    l.security_balance, 
    l.security_nonce, 
    l.balance, 
    l.nonce, 
    l.first_block
    :extend
from account_inventory l
:scope
:order
:limit

-- :account_list_order
order by l.first_block desc, l.address

-- :account_list_before_scope
where (l.address > $1 and l.first_block = $2) or (l.first_block < $2)

-- :account_speculative_extend
, (select greatest(l.nonce, coalesce(max(p.nonce), l.nonce)) 
    from pending_transactions p 
    where p.address = l.address and nonce_type='balance' and status != 'failed'
) as speculative_nonce
, (select greatest(l.security_nonce, coalesce(max(p.nonce)), l.security_nonce) 
    from pending_transactions p 
    where p.address = l.address and nonce_type='security' and status != 'failed'
) as speculative_sec_nonce


-- :account_balance_series
with ts as ( 
    select generate_series(
        date_trunc($4::text, $2::timestamptz + $5::interval) - $3::interval,
        date_trunc($4::text, $2::timestamptz + $5::interval), 
        $5::interval) as timestamp
    order by timestamp desc
),
accounts_ts as (
    select 
        accounts.*, 
        blocks.timestamp  
    from accounts inner join blocks on blocks.height = accounts.block
    where address = $1
)
select 
    ts.timestamp, 
    (select accounts_ts.balance 
        from accounts_ts 
        where timestamp <= ts.timestamp 
        order by timestamp desc limit 1) 
from ts
