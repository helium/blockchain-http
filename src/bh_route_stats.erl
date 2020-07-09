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

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_STATS_BLOCK_TIMES,
                            [
                             "with month_interval as (",
                             "    select to_timestamp(time) as timestamp, ",
                             "           time - (lead(time) over (order by height desc)) as diff_time",
                             "    from blocks",
                             "    where to_timestamp(time) > (now() - '1 month'::interval)",
                             "), ",
                             "week_interval as (",
                             "    select * from month_interval where timestamp > (now() - '1 week'::interval)",
                             "), ",
                             "day_interval as (",
                             "    select * from week_interval where timestamp > (now() - '24 hour'::interval)",
                             "), ",
                             "hour_interval as (",
                             "    select * from day_interval where timestamp > (now() - '1 hour'::interval)",
                             ") ",
                             "select ",
                             "    (select avg(diff_time) from hour_interval) as last_hour_avg,",
                             "    (select avg(diff_time) from day_interval) as last_day_avg,",
                             "    (select avg(diff_time) from week_interval) as last_week_avg,",
                             "    (select avg(diff_time) from month_interval) as last_month_avg,",
                             %% Add std deviations
                             "    (select stddev(diff_time) from hour_interval) as last_hour_stddev,",
                             "    (select stddev(diff_time) from day_interval) as last_day_stddev,",
                             "    (select stddev(diff_time) from week_interval) as last_week_stddev,",
                             "    (select stddev(diff_time) from month_interval) as last_month_stddev"
                            ], []),

    {ok, S2} = epgsql:parse(Conn, ?S_STATS_ELECTION_TIMES,
                            [
                             "with month_interval as (",
                             "    select to_timestamp(time) as timestamp, ",
                             "           time - (lead(time) over (order by block desc)) as diff_time",
                             "    from transactions",
                             "    where to_timestamp(time) > (now() - '1 month'::interval)",
                             "    and type = 'consensus_group_v1'"
                             "), ",
                             "week_interval as (",
                             "    select * from month_interval where timestamp > (now() - '1 week'::interval)",
                             "), ",
                             "day_interval as (",
                             "    select * from week_interval where timestamp > (now() - '24 hour'::interval)",
                             "), ",
                             "hour_interval as (",
                             "    select * from day_interval where timestamp > (now() - '1 hour'::interval)",
                             ") ",
                             "select ",
                             "    (select avg(diff_time) from hour_interval) as last_hour_avg,",
                             "    (select avg(diff_time) from day_interval) as last_day_avg,",
                             "    (select avg(diff_time) from week_interval) as last_week_avg,",
                             "    (select avg(diff_time) from month_interval) as last_month_avg,",
                             %% Add std deviations
                             "    (select stddev(diff_time) from hour_interval) as last_hour_stddev,",
                             "    (select stddev(diff_time) from day_interval) as last_day_stddev,",
                             "    (select stddev(diff_time) from week_interval) as last_week_stddev,",
                             "    (select stddev(diff_time) from month_interval) as last_month_stddev"
                            ], []),

    {ok, S3} = epgsql:parse(Conn, ?S_TOKEN_SUPPLY,
                            "select (sum(balance) / 100000000) as token_supply from account_inventory",
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_STATS_STATE_CHANNELS,
                            [" with month_interval as (",
                             "     select to_timestamp(b.time) as timestamp, ",
                             "        state_channel_counts(t.type, t.fields) as counts",
                             "     from blocks b inner join transactions t on b.height = t.block",
                             "     where to_timestamp(b.time) > (now() - '1 month'::interval)",
                             "         and t.type = 'state_channel_close_v1'",
                             " ),",
                             " week_interval as (",
                             "     select * from month_interval where timestamp > (now() - '1 week'::interval)",
                             " ),",
                             " day_interval as (",
                             "     select * from week_interval where timestamp > (now() - '24 hour'::interval)",
                             " )",
                             " select",
                             "     (select sum((t.counts).num_dcs) as num_dcs from day_interval t) as last_day_dcs,",
                             "     (select sum((t.counts).num_packets) as num_dcs from day_interval t) as last_day_packets,",
                             "     (select sum((t.counts).num_dcs) as num_dcs from week_interval t) as last_week_dcs,",
                             "     (select sum((t.counts).num_packets) as num_dcs from week_interval t) as last_week_packets,",
                             "     (select sum((t.counts).num_dcs) as num_dcs from month_interval t) as last_month_dcs,",
                             "     (select sum((t.counts).num_packets) as num_dcs from month_interval t) as last_month_packets;"
                             ], []),

    {ok, S5} = epgsql:parse(Conn, ?S_STATS_COUNTS,
                            "select name, value from stats_inventory",
                            []),


    {ok, S6} = epgsql:parse(Conn, ?S_STATS_CHALLENGES,
                            ["with block_poc_range as ( ",
                             "        select greatest(0, max(height) - coalesce((select value::bigint from vars_inventory where name = 'poc_challenge_interval'), 30)) as min, ",
                             "               max(height) ",
                             "        from blocks ",
                             "), ",
                             "block_last_day_range as ( ",
                             "    select min(height), max(height) from blocks ",
                             "    where timestamp between now() - '24 hour'::interval and now() ",
                             "), ",
                             "last_day_challenges as ( ",
                             "    select hash from transactions ",
                             "    where block between (select min from block_last_day_range) and (select max from block_last_day_range) and type = 'poc_receipts_v1'",
                             "), ",
                             "poc_receipts as ( ",
                             "    select hash, fields->>'onion_key_hash' as challenge_id from transactions ",
                             "    where block between (select min from block_poc_range) and (select max from block_poc_range) and type = 'poc_receipts_v1' ",
                             "), ",
                             "poc_requests as ( ",
                             "    select hash, fields->>'onion_key_hash' as challenge_id from transactions ",
                             "    where block between (select min from block_poc_range) and (select max from block_poc_range) and type = 'poc_request_v1'",
                             ") ",
                             "select * from ",
                             "    (select count(*) as active_challenges from poc_requests ",
                             "     where challenge_id not in (select challenge_id from poc_receipts)) as active, ",
                             "    (select count(*) as last_day_challenges from last_day_challenges) as last_day "
                            ], []),
    #{
      ?S_STATS_BLOCK_TIMES => S1,
      ?S_STATS_ELECTION_TIMES => S2,
      ?S_TOKEN_SUPPLY => S3,
      ?S_STATS_STATE_CHANNELS => S4,
      ?S_STATS_COUNTS => S5,
      ?S_STATS_CHALLENGES => S6
     }.

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_stats(), block_time);

handle(_, _, _Req) ->
    ?RESPONSE_404.

get_stats()  ->
    BlockTimeResults = ?PREPARED_QUERY(?S_STATS_BLOCK_TIMES, []),
    ElectionTimeResults = ?PREPARED_QUERY(?S_STATS_ELECTION_TIMES, []),
    StateChannelResults = ?PREPARED_QUERY(?S_STATS_STATE_CHANNELS, []),
    SupplyResult = ?PREPARED_QUERY(?S_TOKEN_SUPPLY, []),
    CountsResults = ?PREPARED_QUERY(?S_STATS_COUNTS, []),
    ChallengeResults = ?PREPARED_QUERY(?S_STATS_CHALLENGES, []),

    {ok, #{
           block_times => mk_stats_from_time_results(BlockTimeResults),
           election_times => mk_stats_from_time_results(ElectionTimeResults),
           token_supply => mk_token_supply_from_result(SupplyResult),
           state_channel_counts => mk_stats_from_state_channel_results(StateChannelResults),
           counts => mk_stats_from_counts_results(CountsResults),
           challenge_counts => mk_stats_from_challenge_results(ChallengeResults)
          }
    }.

mk_stats_from_time_results({ok, _, [{LastHrAvg, LastDayAvg, LastWeekAvg, LastMonthAvg,
                                     LastHrStddev, LastDayStddev, LastWeekStddev, LastMonthStddev}]}) ->
    #{
      last_hour => #{ avg => mk_float(LastHrAvg), stddev => mk_float(LastHrStddev)},
      last_day => #{ avg => mk_float(LastDayAvg), stddev => mk_float(LastDayStddev)},
      last_week => #{ avg => mk_float(LastWeekAvg), stddev => mk_float(LastWeekStddev)},
      last_month => #{ avg => mk_float(LastMonthAvg), stddev => mk_float(LastMonthStddev)}
     }.

mk_stats_from_state_channel_results({ok, _, [{LastDayDCs, LastDayPackets,
                                             LastWeekDCs, LastWeekPackets,
                                             LastMonthDCs, LastMonthPackets}]}) ->
    #{
      last_day => #{ num_packets => mk_int(LastDayPackets), num_dcs => mk_int(LastDayDCs)},
      last_week => #{ num_packets => mk_int(LastWeekPackets), num_dcs => mk_int(LastWeekDCs)},
      last_month => #{ num_packets => mk_int(LastMonthPackets), num_dcs => mk_int(LastMonthDCs)}
     }.

mk_stats_from_counts_results({ok, _, CountsResults}) ->
    maps:from_list(CountsResults).

mk_stats_from_challenge_results({ok, _, [{ActiveChallenges, LastDayChallenges}]}) ->
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



mk_token_supply_from_result({ok, _, [{TokenSupply}]}) ->
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
