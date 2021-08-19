-module(bh_route_dc_burns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
-export([get_burn_stats/0, mk_burn_stats_result/1]).

-define(S_BURN_LIST_BEFORE, "burn_list_before").
-define(S_BURN_LIST, "burn_list").
-define(S_BURN_STATS, "burn_stats").
-define(S_BURN_SUM, "burn_sum").
-define(S_BURN_BUCKETED_SUM, "burn_bucketed_sum").

-define(BURN_LIST_LIMIT, 100).
-define(BURN_LIST_BLOCK_ALIGN, 100).

-define(FILTER_TYPES, [
    <<"add_gateway">>,
    <<"assert_location">>,
    <<"state_channel">>,
    <<"routing">>,
    <<"fee">>
]).

prepare_conn(Conn) ->
    BurnListLimit = "limit " ++ integer_to_list(?BURN_LIST_LIMIT),
    Loads = [
        {?S_BURN_LIST_BEFORE,
            {burn_list_base, [
                {scope, burn_list_before_scope},
                {limit, BurnListLimit}
            ]}},
        {?S_BURN_LIST,
            {burn_list_base, [
                {scope, burn_list_scope},
                {limit, BurnListLimit}
            ]}},
        ?S_BURN_SUM,
        ?S_BURN_BUCKETED_SUM,
        ?S_BURN_STATS
    ],
    bh_db_worker:load_from_eql(Conn, "dc_burns.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    Result = get_burn_list(Args),
    CacheTime = get_burn_list_cache_time(Args),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [<<"stats">>], _Req) ->
    ?MK_RESPONSE(get_stats(), {block_time, 5});
handle('GET', [<<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(get_burn_sum(Args), block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_burn_list([{cursor, undefined}, {filter_types, FilterTypes}]) ->
    Result = ?PREPARED_QUERY(?S_BURN_LIST, [?FILTER_TYPES_TO_SQL(?FILTER_TYPES, FilterTypes)]),
    mk_burn_list_from_result(FilterTypes, Result);
get_burn_list([{cursor, Cursor}, {filter_types, _}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"filter_types">> := FilterTypes
        }} ->
            Result = ?PREPARED_QUERY(?S_BURN_LIST_BEFORE, [
                ?FILTER_TYPES_TO_SQL(?FILTER_TYPES, FilterTypes),
                BeforeAddress,
                BeforeBlock
            ]),
            mk_burn_list_from_result(FilterTypes, Result);
        _ ->
            {error, badarg}
    end.

%% If the request had a cursor in it we can cache the response for that request
%% for a long time since the cursor makes the response stable.
get_burn_list_cache_time([{cursor, undefined}, {filter_types, _}]) ->
    block_time;
get_burn_list_cache_time([{cursor, _}, {filter_types, _}]) ->
    infinity.

mk_burn_list_from_result(FilterTypes, {ok, _, Results}) ->
    {ok, burn_list_to_json(Results), mk_cursor(FilterTypes, Results)}.

mk_cursor(FilterTypes, Results) when is_list(Results) ->
    case length(Results) < ?BURN_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {Height, Actor, _Type, _Amount, _Proce} = lists:last(Results),
            #{
                filter_types => FilterTypes,
                before_address => Actor,
                before_block => Height
            }
    end.

get_stats() ->
    Results = get_burn_stats(),
    Now = calendar:universal_time(),
    Meta = #{
        timestamp => iso8601:format(Now)
    },
    {ok, mk_burn_stats_result(Results), undefined, Meta}.

get_burn_stats() ->
    bh_cache:get(
        {?MODULE, burn_stats},
        fun() ->
            {ok, _, Data} = ?PREPARED_QUERY(?S_BURN_STATS, []),
            Data
        end
    ).

get_burn_sum([
    {max_time, MaxTime0},
    {min_time, MinTime0},
    {bucket, undefined}
]) ->
    case ?PARSE_TIMESPAN(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            Result = ?PREPARED_QUERY(?S_BURN_SUM, [MinTime, MaxTime]),
            Meta = #{
                max_time => iso8601:format(MaxTime),
                min_time => iso8601:format(MinTime)
            },
            {ok, mk_burn_sum_result(Result), undefined, Meta};
        {error, _} = Error ->
            Error
    end;
get_burn_sum([
    {max_time, MaxTime0},
    {min_time, MinTime0},
    {bucket, Bucket}
]) ->
    case ?PARSE_BUCKETED_TIMESPAN(MaxTime0, MinTime0, Bucket) of
        {ok, {{MaxTime, MinTime}, {BucketType, BucketStep}}} ->
            Result = ?PREPARED_QUERY(?S_BURN_BUCKETED_SUM, [MaxTime, MinTime, BucketStep]),
            Meta = #{
                max_time => iso8601:format(MaxTime),
                min_time => iso8601:format(MinTime),
                bucket => BucketType
            },
            BucketResults = mk_burn_stats_result(Result),
            {_, Buckets} = lists:unzip(
                lists:reverse(lists:keysort(1, maps:to_list(BucketResults)))
            ),
            {ok, Buckets, undefined, Meta};
        {error, Error} ->
            {error, Error}
    end.

mk_burn_sum_result({ok, _, Results}) ->
    mk_burn_sum_result({ok, Results});
mk_burn_sum_result({ok, Results}) ->
    maps:from_list(Results).

mk_burn_stats_result({ok, _, Results}) ->
    mk_burn_stats_result({ok, Results});
mk_burn_stats_result({ok, Results}) ->
    lists:foldl(
        fun
            ({_Interval, _Type, 0}, Acc) ->
                Acc;
            ({Interval, Type, NumDCs}, Acc) ->
                maps:update_with(
                    Interval,
                    fun(IntervalMap = #{total := Total}) ->
                        maps:put(Type, NumDCs, IntervalMap#{total => Total + NumDCs})
                    end,
                    #{Type => NumDCs, total => NumDCs},
                    Acc
                )
        end,
        #{},
        Results
    ).

%%
%% json
%%

burn_list_to_json(Results) ->
    lists:map(fun burn_to_json/1, Results).

burn_to_json({Block, Actor, Type, Amount, OraclePrice}) ->
    #{
        block => Block,
        address => Actor,
        type => Type,
        amount => Amount,
        oracle_price => OraclePrice
    }.
