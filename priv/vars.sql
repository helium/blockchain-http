-- :var_list
SELECT v.name, v.type, v.value from vars_inventory v order by name

-- :var_get
SELECT v.name, v.type, v.value from vars_inventory v where v.name = $1
