-- :city_list_base
with online_data as (
    select
        last(l.short_city) as short_city, l.long_city,
        last(l.short_state) as short_state, l.long_state,
        last(l.short_country) as short_country, l.long_country,
        last(l.city_id) as city_id,
        count(*) as hotspot_count,
        s.online as online
    from
        locations l inner join gateway_inventory g on g.location = l.location
        left join gateway_status s on s.address = g.address
    :inner_scope
    group by (l.long_country, l.long_state, l.long_city, s.online)
),
data as (
    select
        d.*,
        coalesce(o.hotspot_count, 0) as online_count,
        (d.hotspot_count - coalesce(o.hotspot_count, 0)) as offline_count,
        :rank
    from
        (select
            last(short_city) as short_city, long_city,
            last(short_state) as short_state, long_state,
            last(short_country) as short_country, long_country,
            last(city_id) as city_id,
            coalesce(sum(hotspot_count), 0) as hotspot_count
        from online_data
            group by (long_country, long_state, long_city)) d
        left join online_data o on d.city_id = o.city_id and o.online = 'online'
    )
select
    d.short_city, d.long_city,
    d.short_state, d.long_state,
    d.short_country, d.long_country,
    d.city_id,
    d.hotspot_count, d.online_count, d.offline_count,
    d.rank
from data d
:scope
:order
:limit

-- :city_list_count_order
order by rank desc, city_id

-- :city_list_count_rank
case $1
    when 'hotspot_count' then d.hotspot_count
    when 'online_count' then coalesce(o.hotspot_count, 0)
    when 'offline_count' then (d.hotspot_count - coalesce(o.hotspot_count, 0))
end as rank

-- :city_list_count_before_scope
where rank <= $2 and city_id > $3

-- :city_list_name_order
order by rank, city_id

-- :city_list_name_rank
d.long_city as rank

-- :city_list_name_before_scope
where rank >= $1 and city_id > $2

-- :city_search_order
order by rank desc, city_id

-- :city_search_rank
word_similarity(d.long_city, $1) as rank

-- :city_search_inner_scope
where l.search_city %> lower($1)
