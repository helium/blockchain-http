-- :location_list_base
select 
    l.short_street, l.long_street,
    l.short_city, l.long_city,
    l.short_state, l.long_state,
    l.short_country, l.long_country,
    l.city_id,
    l.location
from locations l
:scope

