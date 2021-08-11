-module(bh_route_state_channels).

-export([prepare_conn/1, handle/3]).
-export([get_state_channel_stats/0]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-define(S_STATE_CHANNEL_STATS, "state_channels_stats").

prepare_conn(Conn) ->
    Loads = [?S_STATE_CHANNEL_STATS],
    bh_db_worker:load_from_eql(Conn, "state_channels.sql", Loads).

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor, limit], Req)),
    Result = bh_route_txns:get_txn_list(Args, ?STATE_CHANNEL_TXN_LIST_LIMIT),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [<<"stats">>], _Req) ->
    ?MK_RESPONSE(
        {ok,
            mk_stats_from_state_channel_results(
                get_state_channel_stats()
            )},
        block_time
    );
handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"state_channel_close_v1">>}].

get_state_channel_stats() ->
    bh_cache:get(
        {?MODULE, state_channel_stats},
        fun() ->
            {ok, _Columns, Data} = ?PREPARED_QUERY(?S_STATE_CHANNEL_STATS, []),
            Data
        end
    ).

mk_stats_from_state_channel_results({ok, [{LastDayChallenges}]}) ->
    #{
        last_day => ?PARSE_INT(LastDayChallenges)
    }.
