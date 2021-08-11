-module(bh_route_assert_locations).

-export([prepare_conn/1, handle/3]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

prepare_conn(_Conn) ->
    #{}.

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor, limit], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"assert_location_v1,assert_location_v2">>}].
