-define(DB_RO_POOL, persistent_term:get(ro_pool)).
-define(DB_RW_POOL, persistent_term:get(rw_pool)).

-define(PREPARED_QUERY(S, A), ?PREPARED_QUERY(?DB_RO_POOL, (S), (A))).
-define(PREPARED_QUERY(P, S, A), bh_db_worker:prepared_query((P), (S), (A))).

-define(EXECUTE_BATCH(S), ?EXECUTE_BATCH(?DB_RO_POOL, (S))).
-define(EXECUTE_BATCH(P, S), bh_db_worker:execute_batch((P), (S))).
