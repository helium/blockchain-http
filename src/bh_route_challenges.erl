-module(bh_route_challenges).

-export([prepare_conn/1, handle/3]).
-export([
    get_challenge_list/2,
    get_challenge_stats/0
]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-define(S_CHALLENGE_STATS, "challenges_stats").

prepare_conn(Conn) ->
    Loads = [?S_CHALLENGE_STATS],
    bh_db_worker:load_from_eql(Conn, "challenges.sql", Loads).

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor, max_time, min_time, limit], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [<<"stats">>], _Req) ->
    ?MK_RESPONSE(
        {ok,
            bh_route_stats:mk_stats_from_challenge_results(
                get_challenge_stats()
            )},
        block_time
    );
handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"poc_receipts_v1">>}].

get_challenge_list({hotspot, Address}, Args) ->
    bh_route_txns:get_actor_txn_list({hotspot, Address}, add_filter_types(Args));
get_challenge_list({account, Address}, Args) ->
    bh_route_txns:get_actor_txn_list({account, Address}, add_filter_types(Args)).

get_challenge_stats() ->
    bh_cache:get(
        {?MODULE, challenge_stats},
        fun() ->
            {ok, _Columns, Data} = ?PREPARED_QUERY(?S_CHALLENGE_STATS, []),
            Data
        end
    ).
