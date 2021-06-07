-module(bh_route_rewards).

-export([prepare_conn/1, handle/3]).
-export([get_full_reward_list/2, get_blockspan/2, get_reward_list/2, get_reward_sum/2]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

% Limit for reward list lengths. Note that changing this will impact paging dupe
% tests.
-define(REWARD_LIST_LIMIT, 100).
-define(S_BLOCK_RANGE, "reward_block_range").
-define(S_REWARD_LIST_HOTSPOT, "reward_list_hotspot").
-define(S_REWARD_LIST_HOTSPOT_REM, "reward_list_hotspot_rem").
-define(S_REWARD_LIST_ACCOUNT, "reward_list_account").
-define(S_REWARD_LIST_ACCOUNT_REM, "reward_list_account_rem").
-define(S_REWARD_SUM_HOTSPOT, "reward_sum_hotstpot").
-define(S_REWARD_SUM_HOTSPOTS, "reward_sum_hotspots").
-define(S_REWARD_SUM_VALIDATOR, "reward_sum_validator").
-define(S_REWARD_SUM_VALIDATORS, "reward_sum_validators").
-define(S_REWARD_SUM_ACCOUNT, "reward_sum_account").
-define(S_REWARD_SUM_NETWORK, "reward_sum_network").
-define(S_REWARD_BUCKETED_SUM_ACCOUNT, "reward_bucketed_sum_account").
-define(S_REWARD_BUCKETED_SUM_HOTSPOT, "reward_bucketed_sum_hotstpot").
-define(S_REWARD_BUCKETED_SUM_HOTSPOTS, "reward_bucketed_sum_hotstpots").
-define(S_REWARD_BUCKETED_SUM_NETWORK, "reward_bucketed_sum_network").
-define(S_REWARD_BUCKETED_SUM_VALIDATOR, "reward_bucketed_sum_validator").
-define(S_REWARD_BUCKETED_SUM_VALIDATORS, "reward_bucketed_sum_validators").

prepare_conn(Conn) ->
    Loads = [
        ?S_BLOCK_RANGE,
        {?S_REWARD_LIST_HOTSPOT,
            {reward_list_base, [
                {fields, {reward_marker_fields, [{marker, "r.transaction_hash"}]}},
                {scope, "where r.gateway = $1"},
                %% Order transactions by hash in a block since a gateway can
                %% only appear once in a transaction
                {marker, "r.transaction_hash"}
            ]}},
        {?S_REWARD_LIST_HOTSPOT_REM,
            {reward_list_rem_base, [
                {fields, {reward_marker_fields, [{marker, "r.transaction_hash"}]}},
                {scope, "where r.gateway = $1"},
                {marker, "r.transaction_hash"}
            ]}},
        {?S_REWARD_LIST_ACCOUNT,
            {reward_list_base, [
                {fields, {reward_marker_fields, [{marker, "r.gateway"}]}},
                {scope, "where r.account = $1"},
                %% order transactions in a block by gateway for an account since
                %% an account can have multiple gateway rewards in the same
                %% transaction
                {marker, "r.gateway"}
            ]}},
        {?S_REWARD_LIST_ACCOUNT_REM,
            {reward_list_rem_base, [
                {fields, {reward_marker_fields, [{marker, "r.gateway"}]}},
                {scope, "where r.account = $1"},
                {marker, "r.gateway"}
            ]}},
        {?S_REWARD_SUM_HOTSPOT,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_SUM_HOTSPOTS,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_sum_validator_source}
            ]}},
        {?S_REWARD_SUM_VALIDATOR,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_SUM_VALIDATORS,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_sum_validator_source}
            ]}},
        {?S_REWARD_SUM_ACCOUNT,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where r.account = $1"},
                {source, reward_sum_hotspot_source}
            ]}},
        {?S_REWARD_BUCKETED_SUM_HOTSPOTS,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_bucketed_hotspot_source}
            ]}},
        {?S_REWARD_BUCKETED_SUM_HOTSPOT,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_BUCKETED_SUM_VALIDATORS,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_bucketed_validator_source}
            ]}},
        {?S_REWARD_BUCKETED_SUM_VALIDATOR,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where r.gateway = $1"},
                {source, "reward_data"}
            ]}},
        {?S_REWARD_BUCKETED_SUM_ACCOUNT,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where r.account = $1"},
                {source, reward_bucketed_hotspot_source}
            ]}},
        {?S_REWARD_SUM_NETWORK,
            {reward_sum_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_sum_time_source}
            ]}},
        {?S_REWARD_BUCKETED_SUM_NETWORK,
            {reward_bucketed_base, [
                {fields, reward_fields},
                {scope, "where true = $1"},
                {source, reward_bucketed_time_source}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "rewards.sql", Loads).

handle('GET', [<<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(
        bh_route_rewards:get_reward_sum(network, Args),
        block_time
    );
handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.

% Helper to fetch a full list of rewards. Do NOT use as part of the API since it
% can take a long time for some hotspots/accounts
get_full_reward_list({hotspot, Address}, Args = [{max_time, _}, {min_time, _}]) ->
    get_full_reward_list([Address], ?S_REWARD_LIST_HOTSPOT, Args);
get_full_reward_list({account, Address}, Args = [{max_time, _}, {min_time, _}]) ->
    get_full_reward_list([Address], ?S_REWARD_LIST_ACCOUNT, Args).

get_full_reward_list(Args, Query, [{max_time, MaxTime}, {min_time, MinTime}]) ->
    case get_blockspan(MaxTime, MinTime) of
        {ok, {_, {MaxBlock, MinBlock}}} ->
            {ok, _, Results} =
                ?PREPARED_QUERY(Query, Args ++ [MinBlock, MaxBlock]),
            {ok, reward_list_to_json(Results), undefined};
        {error, _} = Error ->
            Error
    end.

get_reward_list({hotspot, Address}, Args = [{cursor, _}, {max_time, _}, {min_time, _}]) ->
    get_reward_list([Address], {?S_REWARD_LIST_HOTSPOT, ?S_REWARD_LIST_HOTSPOT_REM}, Args);
get_reward_list({validator, Address}, Args = [{cursor, _}, {max_time, _}, {min_time, _}]) ->
    get_reward_list([Address], {?S_REWARD_LIST_HOTSPOT, ?S_REWARD_LIST_HOTSPOT_REM}, Args);
get_reward_list({account, Address}, Args = [{cursor, _}, {max_time, _}, {min_time, _}]) ->
    get_reward_list([Address], {?S_REWARD_LIST_ACCOUNT, ?S_REWARD_LIST_ACCOUNT_REM}, Args).

%% network
get_reward_sum(
    network,
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([true], ?S_REWARD_SUM_NETWORK, Args);
get_reward_sum(network, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([true], ?S_REWARD_BUCKETED_SUM_NETWORK, Args);
%% all hotspots
get_reward_sum(
    {hotspot, all},
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([true], ?S_REWARD_SUM_HOTSPOTS, Args);
get_reward_sum({hotspot, all}, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([true], ?S_REWARD_BUCKETED_SUM_HOTSPOTS, Args);
%% one hotspot
get_reward_sum(
    {hotspot, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([Address], ?S_REWARD_SUM_HOTSPOT, Args);
get_reward_sum({hotspot, Address}, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([Address], ?S_REWARD_BUCKETED_SUM_HOTSPOT, Args);
%% all validators
get_reward_sum(
    {validator, all},
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([true], ?S_REWARD_SUM_VALIDATORS, Args);
get_reward_sum({validator, all}, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([true], ?S_REWARD_BUCKETED_SUM_VALIDATORS, Args);
%% one validator
get_reward_sum(
    {validator, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([Address], ?S_REWARD_SUM_VALIDATOR, Args);
get_reward_sum({validator, Address}, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([Address], ?S_REWARD_BUCKETED_SUM_VALIDATOR, Args);
%% one account
get_reward_sum(
    {account, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, undefined}]
) ->
    get_reward_sum([Address], ?S_REWARD_SUM_ACCOUNT, Args);
get_reward_sum({account, Address}, Args = [{max_time, _}, {min_time, _}, {bucket, _}]) ->
    get_reward_bucketed_sum([Address], ?S_REWARD_BUCKETED_SUM_ACCOUNT, Args).

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
            %% Empirically a 100k block search is slower than 10, 10k searches
            %% (which in turn is faster than 100, 1k searches).
            %% so cap at 10000 instead of 100000.
            low_block = max(
                EndBlock,
                LowBlock - min(10000, (HighBlock - LowBlock) * 10)
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

-spec get_blockspan(High :: binary(), Low :: binary()) ->
    {ok, {bh_route_handler:timespan(), bh_route_handler:blockspan()}} |
    {error, term()}.
get_blockspan(MaxTime0, MinTime0) ->
    case ?PARSE_TIMESPAN(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            {ok, _, [{HighBlock, LowBlock}]} =
                ?PREPARED_QUERY(?S_BLOCK_RANGE, [MaxTime, MinTime]),
            {ok, {{MaxTime, MinTime}, {HighBlock, LowBlock}}};
        {error, Error} ->
            {error, Error}
    end.

get_reward_list(
    Args,
    {Query, _RemQuery},
    [{cursor, undefined}, {max_time, MaxTime}, {min_time, MinTime}]
) ->
    case get_blockspan(MaxTime, MinTime) of
        {ok, {_, {HighBlock, LowBlock}}} ->
            State = #state{
                high_block = HighBlock,
                end_block = LowBlock,
                %% Aim for block alignment
                low_block = calc_low_block(HighBlock, LowBlock),
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
                % aka "marker"
                maps:get(<<"txn">>, C, undefined),
                State0
            ),
            %% Collect the initial set of results before the highblock
            %% annd start growing from there
            mk_reward_list_result(grow_txn_list(Query, execute_query(Query, State1)));
        _ ->
            {error, badarg}
    end.

get_reward_sum(Args, Query, [{max_time, MaxTime0}, {min_time, MinTime0}, {bucket, _Bucket}]) ->
    case ?PARSE_TIMESPAN(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            Result = ?PREPARED_QUERY(Query, Args ++ [MinTime, MaxTime]),
            mk_reward_sum_result(MaxTime, MinTime, Result);
        {error, _} = Error ->
            Error
    end.

get_reward_bucketed_sum(Args, Query, [
    {max_time, MaxTime0},
    {min_time, MinTime0},
    {bucket, BucketType0}
]) ->
    case ?PARSE_BUCKETED_TIMESPAN(MaxTime0, MinTime0, BucketType0) of
        {ok, {{MaxTime, MinTime}, {BucketType, BucketStep}}} ->
            Result = ?PREPARED_QUERY(
                Query,
                Args ++ [MaxTime, MinTime, BucketStep]
            ),
            mk_reward_buckets_result(MaxTime, MinTime, BucketType, Result);
        {error, Error} ->
            {error, Error}
    end.

mk_reward_list_result(State = #state{results = Results}) when
    length(Results) > ?REWARD_LIST_LIMIT
->
    {Trimmed, _Remainder} = lists:split(?REWARD_LIST_LIMIT, Results),
    {Block, _Hash, _Timestamp, _Account, _Gateway, _Amount, Marker} = lists:last(Trimmed),
    {ok, reward_list_to_json(Trimmed), mk_reward_list_cursor(Block, Marker, State)};
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

mk_reward_buckets_result(MaxTime, MinTime, BucketType, {ok, _, Results}) ->
    Meta = #{
        max_time => iso8601:format(MaxTime),
        min_time => iso8601:format(MinTime),
        bucket => BucketType
    },
    {ok, reward_buckets_to_json(Results), undefined, Meta}.

mk_reward_list_cursor(EndBlock, undefined, #state{end_block = EndBlock}) ->
    undefined;
mk_reward_list_cursor(BeforeBlock, Marker, State = #state{}) ->
    %% Check if we didn't have an anchor block before and we've reached an anchor point
    AnchorBlock =
        case
            (State#state.anchor_block == undefined) and
                (BeforeBlock rem ?REWARD_LIST_BLOCK_ALIGN == 0)
        of
            true -> BeforeBlock;
            false -> State#state.anchor_block
        end,
    lists:foldl(
        fun
            ({_Key, undefined}, Acc) -> Acc;
            ({Key, Value}, Acc) -> Acc#{Key => Value}
        end,
        #{},
        [
            {block, BeforeBlock},
            {end_block, State#state.end_block},
            {txn, Marker},
            {anchor_block, AnchorBlock}
        ]
    ).

%%
%% to_jaon
%%

reward_list_to_json(Results) ->
    lists:map(fun reward_to_json/1, Results).

reward_to_json({Block, Hash, Timestamp, Account, Gateway, Amount, _Marker}) ->
    #{
        <<"block">> => Block,
        <<"hash">> => Hash,
        <<"timestamp">> => iso8601:format(Timestamp),
        <<"account">> => Account,
        <<"gateway">> => Gateway,
        <<"amount">> => Amount
    }.

reward_buckets_to_json(Results) ->
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
