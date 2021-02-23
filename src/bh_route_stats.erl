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
-define(S_STATS_TOKEN_SUPPLY, "stats_token_supply").
-define(S_STATS_COUNTS, "stats_counts").
-define(S_STATS_CHALLENGES, "stats_challenges").
-define(S_STATS_FEES, "stats_fees").

prepare_conn(Conn) ->
    Loads = [
        ?S_STATS_BLOCK_TIMES,
        ?S_STATS_ELECTION_TIMES,
        ?S_STATS_COUNTS,
        ?S_STATS_CHALLENGES,
        ?S_STATS_STATE_CHANNELS,
        ?S_STATS_TOKEN_SUPPLY,
        ?S_STATS_FEES
    ],
    bh_db_worker:load_from_eql(Conn, "stats.sql", Loads).

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_stats(), block_time);
handle('GET', [<<"token_supply">>], Req) ->
    Args = ?GET_ARGS([format], Req),
    get_token_supply(Args, block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_token_supply([{format, Format}], CacheTime) ->
    Result = ?PREPARED_QUERY(?S_STATS_TOKEN_SUPPLY, []),
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
        FeeResults,
        ValidatorResults
    ] =
        ?EXECUTE_BATCH([
            {?S_STATS_BLOCK_TIMES, []},
            {?S_STATS_ELECTION_TIMES, []},
            {?S_STATS_STATE_CHANNELS, []},
            {?S_STATS_TOKEN_SUPPLY, []},
            {?S_STATS_COUNTS, []},
            {?S_STATS_CHALLENGES, []},
            {?S_STATS_FEES, []},
            {bh_route_validators:stats_query(), []}
        ]),

    {ok, ValidatorStats} = bh_route_validators:mk_stats_from_results(ValidatorResults),
    {ok, #{
        block_times => mk_stats_from_time_results(BlockTimeResults),
        election_times => mk_stats_from_time_results(ElectionTimeResults),
        token_supply => mk_token_supply_from_result(SupplyResult),
        state_channel_counts => mk_stats_from_state_channel_results(StateChannelResults),
        counts => mk_stats_from_counts_results(CountsResults),
        challenge_counts => mk_stats_from_challenge_results(ChallengeResults),
        fees => mk_stats_from_fee_results(FeeResults),
        validators => ValidatorStats
    }}.

mk_stats_from_time_results(
    {ok, [
        {LastHrAvg, LastDayAvg, LastWeekAvg, LastMonthAvg, LastHrStddev, LastDayStddev,
            LastWeekStddev, LastMonthStddev}
    ]}
) ->
    #{
        last_hour => #{avg => ?PARSE_FLOAT(LastHrAvg), stddev => ?PARSE_FLOAT(LastHrStddev)},
        last_day => #{avg => ?PARSE_FLOAT(LastDayAvg), stddev => ?PARSE_FLOAT(LastDayStddev)},
        last_week => #{avg => ?PARSE_FLOAT(LastWeekAvg), stddev => ?PARSE_FLOAT(LastWeekStddev)},
        last_month => #{avg => ?PARSE_FLOAT(LastMonthAvg), stddev => ?PARSE_FLOAT(LastMonthStddev)}
    }.

mk_stats_from_state_channel_results(
    {ok, [
        {LastDayDCs, LastDayPackets, LastWeekDCs, LastWeekPackets, LastMonthDCs, LastMonthPackets}
    ]}
) ->
    #{
        last_day => #{num_packets => ?PARSE_INT(LastDayPackets), num_dcs => ?PARSE_INT(LastDayDCs)},
        last_week => #{
            num_packets => ?PARSE_INT(LastWeekPackets),
            num_dcs => ?PARSE_INT(LastWeekDCs)
        },
        last_month => #{
            num_packets => ?PARSE_INT(LastMonthPackets),
            num_dcs => ?PARSE_INT(LastMonthDCs)
        }
    }.

mk_stats_from_fee_results(
    {ok, [
        {LastDayTxnFees, LastDayStakingFees, LastWeekTxnFees, LastWeekStakingFees, LastMonthTxnFees,
            LastMonthStakingFees}
    ]}
) ->
    #{
        last_day => #{
            transaction => ?PARSE_INT(LastDayTxnFees),
            staking => ?PARSE_INT(LastDayStakingFees)
        },
        last_week => #{
            transaction => ?PARSE_INT(LastWeekTxnFees),
            staking => ?PARSE_INT(LastWeekStakingFees)
        },
        last_month => #{
            transaction => ?PARSE_INT(LastMonthTxnFees),
            staking => ?PARSE_INT(LastMonthStakingFees)
        }
    }.

mk_stats_from_counts_results({ok, CountsResults}) ->
    maps:from_list(CountsResults).

mk_stats_from_challenge_results({ok, [{ActiveChallenges, LastDayChallenges}]}) ->
    #{
        active => ?PARSE_INT(ActiveChallenges),
        last_day => ?PARSE_INT(LastDayChallenges)
    }.

mk_token_supply_from_result({ok, _, Result}) ->
    mk_token_supply_from_result({ok, Result});
mk_token_supply_from_result({ok, [{TokenSupply}]}) ->
    ?PARSE_FLOAT(TokenSupply).
