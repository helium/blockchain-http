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


    #{
      ?S_STATS_BLOCK_TIMES => S1,
      ?S_STATS_ELECTION_TIMES => S2,
      ?S_TOKEN_SUPPLY => S3,
      ?S_STATS_STATE_CHANNELS => S4,
      ?S_STATS_COUNTS => S5
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

    {ok, #{
           block_times => mk_stats_from_time_results(BlockTimeResults),
           election_times => mk_stats_from_time_results(ElectionTimeResults),
           token_supply => mk_token_supply_from_result(SupplyResult),
           state_channel_counts => mk_stats_from_state_channel_results(StateChannelResults),
           counts => mk_stats_from_counts_results(CountsResults)
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
    
mk_float(null) ->
    null;
mk_float(Bin) ->
    binary_to_float(Bin).

mk_int(null) ->
    null;
mk_int(Bin) ->
    binary_to_integer(Bin).


mk_token_supply_from_result({ok, _, [{TokenSupply}]}) ->
    mk_float(TokenSupply).
