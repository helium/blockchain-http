-module(bh_route_elections).

-export([prepare_conn/1, handle/3]).
-export([get_election_list/2]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

prepare_conn(_Conn) ->
    #{}.

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"consensus_group_v1">>}].

get_election_list({hotspot, Address}, Args = [{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({hotspot, Address}, add_filter_types(Args));
get_election_list({account, Address}, Args = [{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({account, Address}, add_filter_types(Args)).
