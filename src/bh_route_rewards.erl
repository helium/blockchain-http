-module(bh_route_rewards).

-export([prepare_conn/1, handle/3]).
-export([get_reward_list/2, get_reward_sum/2, get_reward_stats/2]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-define(REWARD_LIST_LIMIT, 100).
-define(S_BLOCK_RANGE, "reward_block_range").
-define(S_REWARD_LIST_HOTSPOT, "reward_list_hotspot").
-define(S_REWARD_LIST_HOTSPOT_REM, "reward_list_hotspot_rem").
-define(S_REWARD_LIST_ACCOUNT, "reward_list_account").
-define(S_REWARD_LIST_ACCOUNT_REM, "reward_list_account_rem").
-define(S_REWARD_SUM_HOTSPOT, "reward_sum_hotstpot").
-define(S_REWARD_SUM_ACCOUNT, "reward_sum_account").
-define(S_REWARD_STATS_HOTSPOTS, "reward_stats_hotstpots").
-define(S_REWARD_STATS_HOTSPOT, "reward_stats_hotstpot").
-define(S_REWARD_STATS_ACCOUNT, "reward_stats_account").
-define(REWARD_FIELDS,
    "r.block, r.transaction_hash, to_timestamp(r.time) as timestamp, r.account, r.gateway, r.amount"
).

prepare_conn(Conn) ->
    Loads = [
        ?S_BLOCK_RANGE,
        {?S_REWARD_LIST_HOTSPOT,
            {reward_list_base, [{fields, ?REWARD_FIELDS}, {scope, "where r.gateway = $1"}]}},
        {?S_REWARD_LIST_HOTSPOT_REM,
            {reward_list_rem_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.gateway = $1"}
            ]}},
        {?S_REWARD_LIST_ACCOUNT,
            {reward_list_base, [{fields, ?REWARD_FIELDS}, {scope, "where r.account = $1"}]}},
        {?S_REWARD_LIST_ACCOUNT_REM,
            {reward_list_rem_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.account = $1"}
            ]}},
        {?S_REWARD_SUM_HOTSPOT,
            {reward_sum_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_SUM_ACCOUNT,
            {reward_sum_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.account = $1"},
                {source, {eql, reward_sum_hotspot_group}}
            ]}},
        {?S_REWARD_STATS_HOTSPOTS,
            {reward_stats_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where true = $1"},
                {source, {eql, reward_stats_hotspot_group}}
            ]}},
        {?S_REWARD_STATS_HOTSPOT,
            {reward_stats_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_STATS_ACCOUNT,
            {reward_stats_base, [
                {fields, ?REWARD_FIELDS},
                {scope, "where r.account = $1"},
                {source, {eql, reward_stats_hotspot_group}}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "rewards.sql", Loads).

handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

get_reward_list({hotspot, Address}, Args = [{cursor, _}, {max_time, _}, {min_time, _}]) ->
    get_reward_list([Address], {?S_REWARD_LIST_HOTSPOT, ?S_REWARD_LIST_HOTSPOT_REM}, Args);
get_reward_list({account, Address}, Args = [{cursor, _}, {max_time, _}, {min_time, _}]) ->
    get_reward_list([Address], {?S_REWARD_LIST_ACCOUNT, ?S_REWARD_LIST_ACCOUNT_REM}, Args).

get_reward_sum({hotspot, Address}, Args = [{max_time, _}, {min_time, _}]) ->
    get_reward_sum([Address], ?S_REWARD_SUM_HOTSPOT, Args);
get_reward_sum({account, Address}, Args = [{max_time, _}, {min_time, _}]) ->
    get_reward_sum([Address], ?S_REWARD_SUM_ACCOUNT, Args).

get_reward_stats(
    {hotspot, all},
    Args = [{max_time, _}, {min_time, _}, {bucket, _}]
) ->
    get_reward_stats([true], ?S_REWARD_STATS_HOTSPOT, Args);
get_reward_stats(
    {hotspot, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, _}]
) ->
    get_reward_stats([Address], ?S_REWARD_STATS_HOTSPOT, Args);
get_reward_stats(
    {account, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, _}]
) ->
    get_reward_stats([Address], ?S_REWARD_STATS_ACCOUNT, Args).

-define(REWARD_LIST_BLOCK_ALIGN, 100).

-record(state, {
    anchor_block = undefined :: pos_integer() | undefined,
    high_block :: pos_integer(),
    end_block :: pos_integer(),
    low_block :: pos_integer(),
    args :: [term()],
    results = [] :: [term()]
}).

%% Grows a txn list with the given query until it's the txn list limit
%% size. We 10x the search space every time we find we don't have
%% enough transactions.
grow_txn_list(_Query, State = #state{results = Results}) when
    length(Results) >= ?REWARD_LIST_LIMIT
->
    State;
grow_txn_list(_Query, State = #state{low_block = LowBlock, end_block = EndBlock}) when
    LowBlock == EndBlock
->
    State;
grow_txn_list(_Query, #state{low_block = LowBlock, high_block = HighBlock}) when
    LowBlock == HighBlock
->
    error(bad_arg);
grow_txn_list(
    Query,
    State = #state{low_block = LowBlock, high_block = HighBlock, end_block = EndBlock}
) ->
    NewState = execute_query(
        Query,
        State#state{
            high_block = LowBlock,
            low_block = max(
                EndBlock,
                LowBlock - (HighBlock - LowBlock) * 10
            )
        }
    ),
    grow_txn_list(Query, NewState).

execute_query(Query, State) ->
    AddedArgs = [State#state.low_block, State#state.high_block],
    {ok, _, Results} = ?PREPARED_QUERY(Query, State#state.args ++ AddedArgs),
    State#state{results = State#state.results ++ Results}.

execute_rem_query(_Query, _HighBlock, undefined, State) ->
    State;
execute_rem_query(Query, HighBlock, TxnHash, State) ->
    AddedArgs = [HighBlock, TxnHash],
    {ok, _, Results} = ?PREPARED_QUERY(Query, State#state.args ++ AddedArgs),
    State#state{results = State#state.results ++ Results}.

calc_low_block(HighBlock, EndBlock) ->
    case HighBlock - HighBlock rem ?REWARD_LIST_BLOCK_ALIGN of
        HighBlock ->
            max(EndBlock, HighBlock - ?REWARD_LIST_BLOCK_ALIGN);
        Other ->
            max(EndBlock, Other)
    end.

-spec parse_min_max_time(High :: binary(), Low :: binary()) ->
    {ok, {MaxTime :: calendar:datetime(), MinTime :: calendar:datetime()}} |
    {error, term()}.
parse_min_max_time(MaxTime, MinTime) when
    MaxTime == undefined orelse MinTime == undefined
->
    {error, badarg};
parse_min_max_time(MaxTime0, MinTime0) ->
    try
        MaxTime = iso8601:parse(MaxTime0),
        MinTime = iso8601:parse(MinTime0),
        {ok, {MaxTime, MinTime}}
    catch
        error:badarg ->
            {error, badarg}
    end.

-spec get_min_max_height(High :: binary(), Low :: binary()) ->
    {ok,
        {{MaxTime :: calendar:datetime(), HighBlock :: pos_integer()},
            {MinTime :: calendar:datetime(), LowBlock :: pos_integer()}}} |
    {error, term()}.
get_min_max_height(MaxTime0, MinTime0) ->
    case parse_min_max_time(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            {ok, _, [{HighBlock, LowBlock}]} =
                ?PREPARED_QUERY(?S_BLOCK_RANGE, [MaxTime, MinTime]),
            {ok, {{MaxTime, HighBlock}, {MinTime, LowBlock}}};
        {error, Error} ->
            {error, Error}
    end.

get_reward_list(
    Args,
    {Query, _RemQuery},
    [{cursor, undefined}, {max_time, MaxTime}, {min_time, MinTime}]
) ->
    case get_min_max_height(MaxTime, MinTime) of
        {ok, {{_, HighBlock}, {_, EndBlock}}} ->
            State = #state{
                high_block = HighBlock,
                end_block = EndBlock,
                %% Aim for block alignment
                low_block = calc_low_block(HighBlock, EndBlock),
                args = Args
            },
            mk_reward_list_result(execute_query(Query, State));
        {error, _} = Error ->
            Error
    end;
get_reward_list(
    Args,
    {Query, RemQuery},
    [{cursor, Cursor}, {max_time, _}, {min_time, _}]
) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, C = #{<<"block">> := HighBlock, <<"end_block">> := EndBlock}} ->
            State0 = #state{
                high_block = HighBlock,
                end_block = EndBlock,
                anchor_block = maps:get(<<"anchor_block">>, C, undefined),
                low_block = calc_low_block(HighBlock, EndBlock),
                args = Args
            },
            %% Construct the a partial list of results if we were
            %% partway into the block
            State1 = execute_rem_query(
                RemQuery,
                HighBlock,
                maps:get(<<"txn">>, C, undefined),
                State0
            ),
            %% Collect the initial set of results before the highblock
            %% annd start growing from there
            mk_reward_list_result(grow_txn_list(Query, execute_query(Query, State1)));
        _ ->
            {error, badarg}
    end.

get_reward_sum(Args, Query, [{max_time, MaxTime0}, {min_time, MinTime0}]) ->
    case get_min_max_height(MaxTime0, MinTime0) of
        {ok, {{MaxTime, HighBlock}, {MinTime, LowBlock}}} ->
            Result = ?PREPARED_QUERY(Query, Args ++ [LowBlock, HighBlock]),
            mk_reward_sum_result(MaxTime, MinTime, Result);
        {error, _} = Error ->
            Error
    end.

get_bucket_args(<<"month">>) ->
    {ok, {<<"month">>, {{0, 0, 0}, 0, 1}}};
get_bucket_args(<<"week">>) ->
    {ok, {<<"week">>, {{0, 0, 0}, 7, 0}}};
get_bucket_args(<<"day">>) ->
    {ok, {<<"day">>, {{0, 0, 0}, 1, 0}}};
get_bucket_args(<<"hour">>) ->
    {ok, {<<"hour">>, {{1, 0, 0}, 0, 0}}};
get_bucket_args(_) ->
    {error, badarg}.

get_reward_stats(
    Args,
    Query,
    [
        {max_time, MaxTime0},
        {min_time, MinTime0},
        {bucket, BucketType0}
    ]
) ->
    case parse_min_max_time(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            case get_bucket_args(BucketType0) of
                {ok, {BucketType, BucketStep}} ->
                    Result = ?PREPARED_QUERY(
                        Query,
                        Args ++ [MinTime, MaxTime, BucketType, BucketStep]
                    ),
                    mk_reward_stats_result(MaxTime, MinTime, BucketType, Result);
                {error, _} = Error ->
                    Error
            end;
        {error, Error} ->
            {error, Error}
    end.

mk_reward_list_result(State = #state{results = Results}) when
    length(Results) > ?REWARD_LIST_LIMIT
->
    {Trimmed, _Remainder} = lists:split(?REWARD_LIST_LIMIT, Results),
    {Block, Hash, _Timestamp, _Account, _Gateway, _Amount} = lists:last(Trimmed),
    {ok, reward_list_to_json(Trimmed), mk_reward_list_cursor(Block, Hash, State)};
mk_reward_list_result(State = #state{results = Results}) ->
    {ok, reward_list_to_json(Results),
        mk_reward_list_cursor(State#state.low_block, undefined, State)}.

mk_reward_sum_result(MaxTime, MinTime, {ok, _, [Result]}) ->
    Meta = #{
        max_time => iso8601:format(MaxTime),
        min_time => iso8601:format(MinTime)
    },
    %% Result is expected to have the same fields as a stat results
    {ok, reward_stat_to_json(Result), undefined, Meta}.

mk_reward_stats_result(MaxTime, MinTime, BucketType, {ok, _, Results}) ->
    Meta = #{
        max_time => iso8601:format(MaxTime),
        min_time => iso8601:format(MinTime),
        bucket => BucketType
    },
    {ok, reward_stats_to_json(Results), undefined, Meta}.

mk_reward_list_cursor(EndBlock, undefined, #state{end_block = EndBlock}) ->
    undefined;
mk_reward_list_cursor(BeforeBlock, BeforeAddr, State = #state{}) ->
    %% Check if we didn't have an anchor block before and we've reached an anchor point
    AnchorBlock =
        case
            (State#state.anchor_block == undefined) and
                (BeforeBlock rem ?REWARD_LIST_BLOCK_ALIGN == 0)
        of
            true ->
                BeforeBlock;
            false ->
                State#state.anchor_block
        end,
    lists:foldl(
        fun
            ({_Key, undefined}, Acc) ->
                Acc;
            ({Key, Value}, Acc) ->
                Acc#{Key => Value}
        end,
        #{},
        [
            {block, BeforeBlock},
            {end_block, State#state.end_block},
            {txn, BeforeAddr},
            {anchor_block, AnchorBlock}
        ]
    ).

%%
%% to_jaon
%%

reward_list_to_json(Results) ->
    lists:map(fun reward_to_json/1, Results).

reward_to_json({Block, Hash, Timestamp, Account, Gateway, Amount}) ->
    #{
        <<"block">> => Block,
        <<"hash">> => Hash,
        <<"timestamp">> => iso8601:format(Timestamp),
        <<"account">> => Account,
        <<"gateway">> => Gateway,
        <<"amount">> => Amount
    }.

reward_stats_to_json(Results) ->
    lists:map(fun reward_stat_to_json/1, Results).

reward_stat_to_json({Min, Max, Sum, Total, Median, Avg, StdDev}) ->
    #{
        min => Min,
        max => Max,
        %% keep DC sum for API backwards compatibility until deprecated
        sum => Sum,
        total => Total,
        median => Median,
        avg => Avg,
        stddev => StdDev
    };
reward_stat_to_json({Timestamp, Min, Max, Sum, Total, Median, Avg, StdDev}) ->
    Base = reward_stat_to_json({Min, Max, Sum, Total, Median, Avg, StdDev}),
    Base#{
        timestamp => iso8601:format(Timestamp)
    }.
