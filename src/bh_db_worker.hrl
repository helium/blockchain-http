-define(DB_RO_POOL, persistent_term:get(ro_pool)).
-define(DB_RW_POOL, persistent_term:get(rw_pool)).

-define(SQUERY(S), ?SQUERY(?DB_RO_POOL, (S))).
-define(EQUERY(S, A), ?EQUERY(?DB_RO_POOL, (S), (A))).
-define(PREPARED_QUERY(S, A), ?PREPARED_QUERY(?DB_RO_POOL, (S), (A))).

-define(SQUERY(P, S), bh_db_worker:squery((P),(S))).
-define(EQUERY(P, S, A), bh_db_worker:equery((P), (S), (A))).
-define(PREPARED_QUERY(P, S, A), bh_db_worker:prepared_query((P), (S), (A))).
