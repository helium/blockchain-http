-module(bh_route_stats).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_stats/0]).

-define(S_STATS_BLOCK_TIMES, "stats_block_times").
-define(S_STATS_ELECTION_TIMES, "stats_election_times").
-define(S_STATS_STATE_CHANNELS, "stats_state_channels").
-define(S_TOKEN_SUPPLY, "stats_token_supply").
-define(S_STATS_COUNTS, "stats_counts").
-define(S_STATS_CHALLENGES, "stats_challenges").
-define(S_STATS_FEES, "stats_fees").

prepare_conn(Conn) ->
    PrivDir = code:priv_dir(blockchain_http),
    {ok, Queries} = eql:compile(filename:join(PrivDir, "stats.sql")),
    Statements = lists:map(
        fun({Name, Query}) ->
            Key = atom_to_list(Name),
            {ok, Statement} = epgsql:parse(Conn, Key, Query, []),
            {Key, Statement}
        end,
        Queries
    ),
    maps:from_list(Statements).

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_stats(), block_time);
handle('GET', [<<"token_supply">>], Req) ->
    Args = ?GET_ARGS([format], Req),
    get_token_supply(Args, block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_token_supply([{format, Format}], CacheTime) ->
    Result = ?PREPARED_QUERY(?S_TOKEN_SUPPLY, []),
    case Format of
        <<"raw">> ->
            Headers = [{<<"Content-Type">>, <<"text/plain">>}],
            {ok, bh_route_handler:add_cache_header(CacheTime, Headers),
                float_to_binary(mk_token_supply_from_result(Result), [{decimals, 8}, compact])};
        _ ->
            ?MK_RESPONSE({ok, #{token_supply => mk_token_supply_from_result(Result)}}, CacheTime)
    end.

get_stats() ->
    [
        BlockTimeResults,
        ElectionTimeResults,
        StateChannelResults,
        SupplyResult,
        CountsResults,
        ChallengeResults,
        FeeResults
    ] =
        ?EXECUTE_BATCH([
            {?S_STATS_BLOCK_TIMES, []},
            {?S_STATS_ELECTION_TIMES, []},
            {?S_STATS_STATE_CHANNELS, []},
            {?S_TOKEN_SUPPLY, []},
            {?S_STATS_COUNTS, []},
            {?S_STATS_CHALLENGES, []},
            {?S_STATS_FEES, []}
        ]),

    {ok, #{
        block_times => mk_stats_from_time_results(BlockTimeResults),
        election_times => mk_stats_from_time_results(ElectionTimeResults),
        token_supply => mk_token_supply_from_result(SupplyResult),
        state_channel_counts => mk_stats_from_state_channel_results(StateChannelResults),
        counts => mk_stats_from_counts_results(CountsResults),
        challenge_counts => mk_stats_from_challenge_results(ChallengeResults),
        fees => mk_stats_from_fee_results(FeeResults)
    }}.

mk_stats_from_time_results(
    {ok, [
        {LastHrAvg, LastDayAvg, LastWeekAvg, LastMonthAvg, LastHrStddev, LastDayStddev,
            LastWeekStddev, LastMonthStddev}
    ]}
) ->
    #{
        last_hour => #{avg => mk_float(LastHrAvg), stddev => mk_float(LastHrStddev)},
        last_day => #{avg => mk_float(LastDayAvg), stddev => mk_float(LastDayStddev)},
        last_week => #{avg => mk_float(LastWeekAvg), stddev => mk_float(LastWeekStddev)},
        last_month => #{avg => mk_float(LastMonthAvg), stddev => mk_float(LastMonthStddev)}
    }.

mk_stats_from_state_channel_results(
    {ok, [
        {LastDayDCs, LastDayPackets, LastWeekDCs, LastWeekPackets, LastMonthDCs, LastMonthPackets}
    ]}
) ->
    #{
        last_day => #{num_packets => mk_int(LastDayPackets), num_dcs => mk_int(LastDayDCs)},
        last_week => #{num_packets => mk_int(LastWeekPackets), num_dcs => mk_int(LastWeekDCs)},
        last_month => #{num_packets => mk_int(LastMonthPackets), num_dcs => mk_int(LastMonthDCs)}
    }.

mk_stats_from_fee_results(
    {ok, [
        {LastDayTxnFees, LastDayStakingFees, LastWeekTxnFees, LastWeekStakingFees, LastMonthTxnFees,
            LastMonthStakingFees}
    ]}
) ->
    #{
        last_day => #{
            transaction => mk_int(LastDayTxnFees),
            staking => mk_int(LastDayStakingFees)
        },
        last_week => #{
            transaction => mk_int(LastWeekTxnFees),
            staking => mk_int(LastWeekStakingFees)
        },
        last_month => #{
            transaction => mk_int(LastMonthTxnFees),
            staking => mk_int(LastMonthStakingFees)
        }
    }.

mk_stats_from_counts_results({ok, CountsResults}) ->
    maps:from_list(CountsResults).

mk_stats_from_challenge_results({ok, [{ActiveChallenges, LastDayChallenges}]}) ->
    #{
        active => mk_int(ActiveChallenges),
        last_day => mk_int(LastDayChallenges)
    }.

mk_float(null) ->
    null;
mk_float(Bin) when is_binary(Bin) ->
    binary_to_float(Bin);
mk_float(Num) when is_float(Num) ->
    Num.

mk_int(null) ->
    null;
mk_int(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
mk_int(Num) when is_integer(Num) ->
    Num.

mk_token_supply_from_result({ok, _, Result}) ->
    mk_token_supply_from_result({ok, Result});
mk_token_supply_from_result({ok, [{TokenSupply}]}) ->
    mk_float(TokenSupply).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

mk_int_test() ->
    ?assertEqual(null, mk_int(null)),
    ?assertEqual(22, mk_int(<<"22">>)),
    ?assertEqual(22, mk_int(22)),
    ok.

mk_float_test() ->
    ?assertEqual(null, mk_float(null)),
    ?assertEqual(22.2, mk_float(<<"22.2">>)),
    ?assertEqual(22.2, mk_float(22.2)),
    ok.

-endif.
