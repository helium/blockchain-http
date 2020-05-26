-module(bh_route_challenges).

-export([prepare_conn/1, handle/3]).
-export([get_challenge_list/2]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

prepare_conn(_Conn) ->
    #{}.

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    ?MK_RESPONSE(bh_route_txns:get_txn_list(?CHALLENGE_LIST_BLOCK_LIMIT, Args),
                 ?CACHE_TIME_BLOCK_ALIGNED(Args));

handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.


add_filter_types(Args) ->
    Args ++ [{filter_types, <<"poc_receipts_v1">>}].

get_challenge_list({hotspot, Address}, Args=[{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({actor, Address}, ?CHALLENGE_ACTOR_LIST_BLOCK_LIMIT, add_filter_types(Args));
get_challenge_list({account, Address}, Args=[{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({owned, Address}, ?CHALLENGE_ACTOR_LIST_BLOCK_LIMIT, add_filter_types(Args)).
