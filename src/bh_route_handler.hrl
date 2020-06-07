-include("bh_db_worker.hrl").

-define(RESPONSE_404, {404, [], jiffy:encode(#{error => <<"Not Found">>})}).
-define(RESPONSE_409, {409, [], jiffy:encode(#{error => <<"Conflict">>})}).
-define(RESPONSE_503, {503, [], jiffy:encode(#{ error => <<"Too Busy">>})}).
-define(RESPONSE_503_SHUTDOWN, {503, [], jiffy:encode(#{ error => <<"Stopping">>})}).
-define(RESPONSE_400, ?RESPONSE_400("Bad Request")).
-define(RESPONSE_400(S), {400, [], jiffy:encode(#{error => list_to_binary((S))})}).

-define(MAX_LIMIT, 1000).
-define(DEFAULT_ARG_LIMIT, <<"100">>).

-define(GET_ARGS(A,R), bh_route_handler:get_args((A), (R))).

-define(MK_RESPONSE(R), ?MK_RESPONSE(R, undefined)).
-define(MK_RESPONSE(R,C), bh_route_handler:mk_response((R),(C))).
-define(INSERT_LAT_LON(L, N, F), bh_route_handler:lat_lon((L), (N), (F))).
-define(INSERT_LAT_LON(L, F), bh_route_handler:lat_lon((L), (F))).
-define(CURSOR_ENCODE(M), bh_route_handler:cursor_encode(M)).
-define(CURSOR_DECODE(B), bh_route_handler:cursor_decode(B)).

-define (BIN_TO_B64(B), base64url:encode((B))).
-define (BIN_TO_B58(B), list_to_binary(libp2p_crypto:bin_to_b58((B)))).

-define(SELECT_TXN_FIELDS(F), ["select t.block, t.time, t.hash, t.type, ", (F), " "]).
-define(SELECT_TXN_BASE, ?SELECT_TXN_FIELDS("t.fields")).
-define(TXN_LIST_TO_JSON(R), bh_route_txns:txn_list_to_json((R))).

-define(CACHE_TIME_BLOCK_ALIGNED(A), bh_route_handler:cache_time_block_aligned((A))).

-define(BLOCK_LIST_LIMIT, 100).
-define(BLOCK_TXN_LIST_LIMIT, 50).
-define(CHALLENGE_TXN_LIST_LIMIT, 50).
-define(PENDING_TXN_LIST_LIMIT, 100).
-define(ACTIVITY_LIST_BLOCK_LIMIT, 1000). % range in blocks
-define(ELECTION_LIST_BLOCK_LIMIT, 1000). % range in blocks
-define(CHALLENGE_LIST_BLOCK_LIMIT, 10).  % range in blocks, all challenges
-define(CHALLENGE_ACTOR_LIST_BLOCK_LIMIT, 500). % range in blocks, challenges for hotspot or account
