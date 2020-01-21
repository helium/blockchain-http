-include("bh_db_worker.hrl").

-define(MAX_LIMIT, 1000).
-define(DEFAULT_ARG_LIMIT, <<"100">>).
-define(GET_ARG_LIMIT(R), ?GET_ARG_LIMIT(R, ?DEFAULT_ARG_LIMIT)).
-define(GET_ARG_LIMIT(R,D), min(?MAX_LIMIT, binary_to_integer(elli_request:get_arg(<<"limit">>, (R), (D))))).

-define(GET_ARG_BEFORE(R,D), elli_request:get_arg(<<"before">>, (R), (D))).
-define(GET_ARG_BEFORE(R), elli_request:get_arg(<<"before">>, (R), <<"-1">>)).

-define(MK_RESPONSE(R), bh_route_handler:mk_response((R))).
-define(INSERT_LAT_LON(L, N, F), bh_route_handler:lat_lon((L), (N), (F))).
-define(INSERT_LAT_LON(L, F), bh_route_handler:lat_lon((L), (F))).
