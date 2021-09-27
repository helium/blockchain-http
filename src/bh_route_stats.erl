-module(bh_route_stats).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([
    get_stats/0,
    get_count_stats/0,
    get_token_supply_stats/0,
    mk_stats_from_time_results/1,
    mk_stats_from_challenge_results/1
]).

-define(S_STATS_TOKEN_SUPPLY, "stats_token_supply").
-define(S_STATS_COUNTS, "stats_counts").

prepare_conn(Conn) ->
    Loads = [
        ?S_STATS_COUNTS,
        ?S_STATS_TOKEN_SUPPLY
    ],
    bh_db_worker:load_from_eql(Conn, "stats.sql", Loads).

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_stats(), {block_time, 1.5});
handle('GET', [<<"counts">>], _Req) ->
    ?MK_RESPONSE(
        {ok,
            mk_stats_from_counts_results(
                get_count_stats()
            )},
        block_time
    );
handle('GET', [<<"token_supply">>], Req) ->
    Args = ?GET_ARGS([format], Req),
    get_token_supply(Args, block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_count_stats() ->
    bh_cache:get(
        {?MODULE, count_stats},
        fun() ->
            {ok, _Columns, Data} = ?PREPARED_QUERY(?S_STATS_COUNTS, []),
            Data
        end
    ).

get_token_supply_stats() ->
    bh_cache:get(
        {?MODULE, token_supply_stats},
        fun() ->
            {ok, _Columns, Data} = ?PREPARED_QUERY(?S_STATS_TOKEN_SUPPLY, []),
            Data
        end
    ).

get_token_supply([{format, Format}], CacheTime) ->
    Result = get_token_supply_stats(),
    case Format of
        <<"raw">> ->
            Headers = [{<<"Content-Type">>, <<"text/plain">>}],
            {ok, bh_route_handler:add_cache_header(CacheTime, Headers),
                float_to_binary(mk_token_supply_from_result(Result), [{decimals, 8}, compact])};
        _ ->
            ?MK_RESPONSE({ok, #{token_supply => mk_token_supply_from_result(Result)}}, CacheTime)
    end.

get_stats() ->
    BlockTimeResults = bh_route_blocks:get_block_stats(),
    ElectionTimeResults = bh_route_elections:get_election_time_stats(),
    SupplyResult = get_token_supply_stats(),
    CountsResults = get_count_stats(),
    ChallengeResults = bh_route_challenges:get_challenge_stats(),

    {ok, #{
        block_times => mk_stats_from_time_results(BlockTimeResults),
        election_times => mk_stats_from_time_results(ElectionTimeResults),
        token_supply => mk_token_supply_from_result(SupplyResult),
        counts => mk_stats_from_counts_results(CountsResults),
        challenge_counts => mk_stats_from_challenge_results(ChallengeResults)
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

mk_stats_from_counts_results({ok, CountsResults}) ->
    maps:from_list(CountsResults).

mk_stats_from_challenge_results({ok, [{ActiveChallenges, LastDayChallenges}]}) ->
    #{
        active => ?PARSE_INT(ActiveChallenges),
        last_day => ?PARSE_INT(LastDayChallenges)
    }.

mk_token_supply_from_result({ok, [{TokenSupply}]}) ->
    ?PARSE_FLOAT(TokenSupply).
