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
    ?MK_RESPONSE(bh_route_txns:get_txn_list(?ELECTION_LIST_BLOCK_LIMIT, Args),
                 ?CACHE_TIME_BLOCK_ALIGNED(Args)).

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"consensus_group_v1">>}].

get_election_list({hotspot, Address}, Args=[{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({actor, Address}, ?ELECTION_LIST_BLOCK_LIMIT, add_filter_types(Args));
get_election_list({account, Address}, Args=[{cursor, _}]) ->
    bh_route_txns:get_actor_txn_list({owned, Address}, ?ELECTION_LIST_BLOCK_LIMIT, add_filter_types(Args)).
