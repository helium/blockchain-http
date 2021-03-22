-- :hotspot_list_base
select
    (select max(height) from blocks) as height,
    g.last_block,
    g.first_block,
    g.first_timestamp,
    g.last_poc_challenge,
    g.address,
    g.owner,
    g.location,
    g.nonce,
    g.name,
    g.reward_scale,
    s.online as online_status,
    s.block as block_status,
    s.listen_addrs as listen_addrs,
    l.short_street, l.long_street,
    l.short_city, l.long_city,
    l.short_state, l.long_state,
    l.short_country, l.long_country,
    l.city_id
    :source
left join locations l on g.location = l.location
left join gateway_status s on s.address = g.address
:scope
:order
:limit

-- :hotspot_list_order
order by g.first_block desc, g.address

-- :hotspot_list_source
from gateway_inventory g

-- :hotspot_list_before_scope
where ((g.address > $1 and g.first_block = $2) or (g.first_block < $2))

-- :owner_hotspot_list_source
from (select * from gateway_inventory where owner = $1) as g

-- :owner_hotspot_list_before_scope
where ((g.address > $2 and g.first_block = $3) or (g.first_block < $3))

-- :city_hotspot_list_before_scope
where l.city_id = $1
and ((g.address > $2 and g.first_block = $3) or (g.first_block < $3))

-- :city_hotspot_list_before_scope
where l.city_id = $1
and ((g.address > $2 and g.first_block = $3) or (g.first_block < $3))

-- :city_hotspot_list_scope
where l.city_id = $1

-- :hotspot_name_search_source
from gateway_inventory g

-- :hotspot_name_search_scope
where g.name %> lower($1)

-- :hotspot_name_search_order
order by word_similarity(g.name, $1) desc, name

-- :hotspot_location_box_search_order
order by g.first_block desc, g.address

-- :hotspot_location_box_search_scope
where ST_Intersects(ST_MakeEnvelope($1, $2, $3, $4, 4326), l.geometry)

-- :hotspot_location_box_search_before_scope
where ST_Intersects(ST_MakeEnvelope($1, $2, $3, $4, 4326), l.geometry)
and ((g.address > $5 and g.first_block = $6) or (g.first_block < $6))

-- :hotspot_location_distance_search_order
order by ST_Distance(ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, l.geometry::geography), g.address

-- :hotspot_location_distance_search_scope
where ST_DWithin(ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, l.geometry::geography, $3)

-- :hotspot_location_distance_search_before_scope
where ST_DWithin(ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, l.geometry::geography, $3)
and ((g.address > $4 and g.first_block = $5) or (g.first_block < $5))

-- :hotspot_witness_list
with last_assert as (
    select t.block as height from transactions t inner join transaction_actors a on t.hash = a.transaction_hash
    where t.type = 'assert_location_v1' and a.actor = $1
    order by t.block desc limit 1
),
five_days as (
    select height from blocks where timestamp > (now() - '5 day'::interval) order by height limit 1
),
min as (
    select GREATEST((select height from last_assert), (select height from five_days)) as height
),
recent_witnesses as (
     select $1 as address, jsonb_merge_agg(witnesses) as witnesses from
        (select *
        from gateways
        where address = $1 and block >= (select height from min)
        order by block) a
 ),
hotspot_witnesses as (
    select r.address as witness_for, w.key as witness, w.value as witness_info
    from recent_witnesses r, jsonb_each(r.witnesses) w
)
:hotspot_select

-- :hotspot_witness_list_source
, g.witness_for, g.witness_info
from (select * from hotspot_witnesses w inner join gateway_inventory i on (w.witness = i.address)) g

-- :hotspot_elected_list
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
:hotspot_select

-- :hotspot_elected_list_scope
where g.address in (select * from members)

-- :hotspot_bucketed_witnesses_source
(select
    (select count(*) from jsonb_object_keys(jsonb_merge_agg(w.witnesses))),
    w.time
from witness_data w
group by w.time, w.address)

-- :hotspot_bucketed_witnesses_base
with time_range as (
    select
        extract(epoch from low)::bigint as low,
        extract(epoch from high)::bigint as high
    from (
        select
            timestamp as low,
            lag(timestamp) over (order by timestamp desc) as high
        from generate_series($2::timestamptz, $3::timestamptz, $4::interval) as timestamp) t
    where high is not null
),
witness_data as (
    select
        g.address,
        g.witnesses,
        g.time
    from gateways g
    :scope
    and g.time >= (select min(low) from time_range) and g.time <= (select max(high) from time_range)
)
select
    to_timestamp(t.low) as timestamp,
    coalesce(min(d.count), 0) as min,
    coalesce(max(d.count), 0) as max,
    coalesce(percentile_cont(0.5) within group (order by d.count), 0)::float as median,
    coalesce(avg(d.count), 0)::float as avg,
    coalesce(stddev(d.count), 0)::float as stddev
from time_range t
    left join :source d
    on d.time >= low and d.time < high
group by t.low
order by t.low desc;

-- :hotspot_bucketed_challenges_source
(select
    count(d.time),
    d.time
from challenge_data d
group by d.time, d.address)

-- :hotspot_bucketed_challenges_base
with time_range as (
    select
        extract(epoch from low)::bigint as low,
        extract(epoch from high)::bigint as high
    from (
        select
            timestamp as low,
            lag(timestamp) over (order by timestamp desc) as high
        from generate_series($2::timestamptz, $3::timestamptz, $4::interval) as timestamp) t
    where high is not null
),
challenge_data as (
    select
        a.actor as address,
        b.time
    from transaction_actors a inner join blocks b on b.height = a.block
    :scope
    and b.time >= (select min(low) from time_range) and b.time <= (select max(high) from time_range)
)
select
    to_timestamp(t.low) as timestamp,
    coalesce(min(d.count), 0) as min,
    coalesce(max(d.count), 0) as max,
    coalesce(sum(d.count), 0) as sum,
    coalesce(percentile_cont(0.5) within group (order by d.count), 0)::float as median,
    coalesce(avg(d.count), 0)::float as avg,
    coalesce(stddev(d.count), 0)::float as stddev
from time_range t
    left join :source d
    on d.time >= low and d.time < high
group by t.low
order by t.low desc;