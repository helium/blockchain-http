-include("bh_db_worker.hrl").

-define(RESPONSE_404, {404, [], <<"Not Found">>}).
-define(RESPONSE_400, {400, [], <<"Bad Request">>}).

-define(MAX_LIMIT, 1000).
-define(DEFAULT_ARG_LIMIT, <<"100">>).

-define(GET_ARGS(A,R), bh_route_handler:get_args((A), (R))).

-define(MK_RESPONSE(R), bh_route_handler:mk_response((R))).
-define(INSERT_LAT_LON(L, N, F), bh_route_handler:lat_lon((L), (N), (F))).
-define(INSERT_LAT_LON(L, F), bh_route_handler:lat_lon((L), (F))).

-define (BIN_TO_B64(B), list_to_binary(base64:encode_to_string((B)))).
-define (BIN_TO_B58(B), list_to_binary(libp2p_crypto:bin_to_b58((B)))).
