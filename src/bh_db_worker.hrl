-define(DB_POOL, db_pool).

-define(SQUERY(S), bh_db_worker:squery((S))).
-define(EQUERY(S, P), bh_db_worker:equery((S), (P))).
-define(PREPARED_QUERY(S, P), bh_db_worker:prepared_query((S), (P))).
