-module(bh_route_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([
    get_txn/1,
    get_txn_list/1,
    get_txn_list_cache_time/1,
    get_actor_txn_list/2,
    get_activity_count/2,
    get_activity_list/2,
    txn_to_json/1,
    txn_list_to_json/1
]).

-define(S_TXN, "txn").
-define(S_TXN_LIST, "txn_list").
-define(S_TXN_LIST_REM, "txn_list_rem").
-define(S_ACTOR_TXN_LIST, "actor_txn_list").
-define(S_ACTOR_TXN_LIST_REM, "actor_txn_list_rem").
-define(S_ACCOUNT_HOTSPOTS_TXN_LIST, "account_hotspots_txn_list").
-define(S_ACCOUNT_HOTSPOTS_TXN_LIST_REM, "account_hotspots_txn_list_rem").
-define(S_ACCOUNT_ACTIVITY_COUNT, "account_activity_count").
-define(S_ACCOUNT_ACTIVITY_LIST, "account_activity_list").
-define(S_ACCOUNT_ACTIVITY_LIST_REM, "account_activity_list_rem").
-define(S_HOTSPOT_ACTIVITY_COUNT, "hotspot_activity_count").
-define(S_HOTSPOT_ACTIVITY_LIST, "hotspot_activity_list").
-define(S_HOTSPOT_ACTIVITY_LIST_REM, "hotspot_activity_list_rem").
-define(S_HOTSPOT_MIN_BLOCK, "hotspot_activity_min_block").
-define(S_ACCOUNT_MIN_BLOCK, "account_activity_min_block").
-define(S_ORACLE_MIN_BLOCK, "oracle_activity_min_block").
-define(S_GENESIS_MIN_BLOCK, "txn_genesis_min_block").
-define(S_LOC, "txn_location").

-define(FILTER_TYPES, [
    <<"coinbase_v1">>,
    <<"security_coinbase_v1">>,
    <<"oui_v1">>,
    <<"state_channel_open_v1">>,
    <<"state_channel_close_v1">>,
    <<"gen_gateway_v1">>,
    <<"routing_v1">>,
    <<"payment_v1">>,
    <<"security_exchange_v1">>,
    <<"consensus_group_v1">>,
    <<"add_gateway_v1">>,
    <<"assert_location_v1">>,
    <<"assert_location_v2">>,
    <<"create_htlc_v1">>,
    <<"redeem_htlc_v1">>,
    <<"poc_request_v1">>,
    <<"poc_receipts_v1">>,
    <<"vars_v1">>,
    <<"rewards_v1">>,
    <<"rewards_v2">>,
    <<"token_burn_v1">>,
    <<"dc_coinbase_v1">>,
    <<"token_burn_exchange_rate_v1">>,
    <<"payment_v2">>,
    <<"price_oracle_v1">>,
    <<"transfer_hotspot_v1">>
]).

prepare_conn(Conn) ->
    Loads = [
        {?S_TXN,
            {txn_list_base, [
                {source, txn_list_source},
                {fields, txn_list_fields},
                {scope, txn_get_scope},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_TXN_LIST,
            {txn_list_base, [
                {source, txn_list_source},
                {fields, txn_list_fields},
                {scope, txn_list_scope},
                {order, txn_list_order},
                {limit, {txn_list_limit, []}}
            ]}},
        {?S_TXN_LIST_REM,
            {txn_list_base, [
                {source, txn_list_rem_source},
                {fields, txn_list_fields},
                {scope, txn_list_rem_scope},
                {order, txn_list_order},
                {limit, {txn_list_limit, []}}
            ]}},
        {?S_ACTOR_TXN_LIST,
            {txn_list_base, [
                {source, {txn_actor_list_source, [{actor_scope, txn_actor_scope}]}},
                {fields, txn_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},
        {?S_ACTOR_TXN_LIST_REM,
            {txn_list_base, [
                {source, {txn_actor_list_rem_source, [{actor_scope, txn_actor_scope}]}},
                {fields, txn_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},

        {?S_ACCOUNT_HOTSPOTS_TXN_LIST,
            {txn_list_base, [
                {source, {txn_actor_list_source, [{actor_scope, txn_owned_hotspot_actor_scope}]}},
                {fields, txn_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},
        {?S_ACCOUNT_HOTSPOTS_TXN_LIST_REM,
            {txn_list_base, [
                {source,
                    {txn_actor_list_rem_source, [{actor_scope, txn_owned_hotspot_actor_scope}]}},
                {fields, txn_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},

        {?S_ACCOUNT_ACTIVITY_LIST,
            {txn_list_base, [
                {source,
                    {txn_actor_list_source, [{actor_scope, txn_account_activity_actor_scope}]}},
                {fields, txn_activity_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},
        {?S_ACCOUNT_ACTIVITY_LIST_REM,
            {txn_list_base, [
                {source,
                    {txn_actor_list_rem_source, [{actor_scope, txn_account_activity_actor_scope}]}},
                {fields, txn_activity_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},

        {?S_HOTSPOT_ACTIVITY_LIST,
            {txn_list_base, [
                {source,
                    {txn_actor_list_source, [{actor_scope, txn_hotspot_activity_actor_scope}]}},
                {fields, txn_activity_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},
        {?S_HOTSPOT_ACTIVITY_LIST_REM,
            {txn_list_base, [
                {source,
                    {txn_actor_list_rem_source, [{actor_scope, txn_hotspot_activity_actor_scope}]}},
                {fields, txn_activity_list_fields},
                {scope, ""},
                {order, txn_list_order},
                {limit, ""}
            ]}},

        {?S_LOC, {txn_location, []}},

        {?S_ACCOUNT_ACTIVITY_COUNT,
            {txn_actor_count_base, [
                {actor_scope, txn_account_activity_actor_scope}
            ]}},
        {?S_HOTSPOT_ACTIVITY_COUNT,
            {txn_actor_count_base, [
                {actor_scope, txn_hotspot_activity_actor_scope}
            ]}},

        {?S_HOTSPOT_MIN_BLOCK, {txn_hotspot_activity_min_block, []}},
        {?S_ACCOUNT_MIN_BLOCK, {txn_account_activity_min_block, []}},
        {?S_ORACLE_MIN_BLOCK, {txn_oracle_activity_min_block, []}},
        {?S_GENESIS_MIN_BLOCK, {txn_genesis_min_block, []}}
    ],

    bh_db_worker:load_from_eql(Conn, "txns.sql", Loads).

handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_txn(TxnHash), infinity);
handle(_, _, _Req) ->
    ?RESPONSE_404.

-spec get_txn(Key :: binary()) -> {ok, jiffy:json_object()} | {error, term()}.
get_txn(Key) ->
    case ?PREPARED_QUERY(?S_TXN, [Key]) of
        {ok, _, [Result]} ->
            {ok, txn_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_txn_list(Args = [{cursor, _}, {filter_types, _}]) ->
    get_txn_list([], {?S_GENESIS_MIN_BLOCK, ?S_TXN_LIST, ?S_TXN_LIST_REM}, Args).

get_actor_txn_list({hotspot, Address}, Args = [{cursor, _}, {filter_types, _}]) ->
    get_txn_list([Address], {?S_HOTSPOT_MIN_BLOCK, ?S_ACTOR_TXN_LIST, ?S_ACTOR_TXN_LIST_REM}, Args);
get_actor_txn_list({oracle, Address}, Args = [{cursor, _}, {filter_types, _}]) ->
    get_txn_list([Address], {?S_ORACLE_MIN_BLOCK, ?S_ACTOR_TXN_LIST, ?S_ACTOR_TXN_LIST_REM}, Args);
get_actor_txn_list({account, Address}, Args = [{cursor, _}, {filter_types, _}]) ->
    get_txn_list(
        [Address],
        {?S_ACCOUNT_MIN_BLOCK, ?S_ACCOUNT_HOTSPOTS_TXN_LIST, ?S_ACCOUNT_HOTSPOTS_TXN_LIST_REM},
        Args
    ).

get_activity_list({account, Account}, Args) ->
    get_txn_list(
        [Account],
        {?S_ACCOUNT_MIN_BLOCK, ?S_ACCOUNT_ACTIVITY_LIST, ?S_ACCOUNT_ACTIVITY_LIST_REM},
        Args
    );
get_activity_list({hotspot, Address}, Args) ->
    get_txn_list(
        [Address],
        {?S_HOTSPOT_MIN_BLOCK, ?S_HOTSPOT_ACTIVITY_LIST, ?S_HOTSPOT_ACTIVITY_LIST_REM},
        Args
    ).

get_activity_count({account, Account}, Args) ->
    get_txn_count([Account], ?S_ACCOUNT_ACTIVITY_COUNT, Args);
get_activity_count({hotspot, Address}, Args) ->
    get_txn_count([Address], ?S_HOTSPOT_ACTIVITY_COUNT, Args).

-define(TXN_LIST_BLOCK_ALIGN, 100).
-define(TXN_LIST_MAX_STEP, 10000).

-record(state, {
    anchor_block = undefined :: pos_integer() | undefined,
    high_block :: pos_integer(),
    low_block :: pos_integer(),
    min_block = 1 :: pos_integer(),
    step = 100 :: pos_integer(),
    args :: list(term()),
    types :: iolist(),
    results = [] :: list(term())
}).

%% Grows a txn list with the given queru until it's the txn list limit
%% size. We 10x the search space (up to a max) every time we find we don't have
%% enough transactions.
grow_txn_list(_Query, State = #state{results = Results}) when length(Results) >= ?TXN_LIST_LIMIT ->
    State;
grow_txn_list(_Query, State = #state{low_block = MinBlock, min_block = MinBlock}) ->
    State;
grow_txn_list(_Query, State = #state{low_block = LowBlock, high_block = HighBlock}) when
    HighBlock - LowBlock >= ?TXN_LIST_MAX_STEP
->
    %% Cap at only one max growth before returning a cursor in an attempt to
    %% shed load sooner
    State;
grow_txn_list(_Query, #state{low_block = LowBlock, high_block = HighBlock, min_block = MinBlock}) when
    LowBlock >= HighBlock orelse MinBlock > LowBlock
->
    error(bad_arg);
grow_txn_list(
    Query,
    State = #state{
        low_block = LowBlock,
        min_block = MinBlock,
        step = Step
    }
) ->
    NewState =
        execute_query(Query, State#state{
            high_block = LowBlock,
            %% Empirically a 100k block search is slower than 10, 10k searches
            %% (which in turn is faster than 100, 1k searches).
            step = min(?TXN_LIST_MAX_STEP, Step * 10),
            low_block = max(MinBlock, LowBlock - Step)
        }),
    grow_txn_list(Query, NewState).

calc_low_block(HighBlock, MinBlock) ->
    case HighBlock - (HighBlock rem ?TXN_LIST_BLOCK_ALIGN) of
        HighBlock -> max(MinBlock, HighBlock - ?TXN_LIST_BLOCK_ALIGN);
        Other -> max(MinBlock, Other)
    end.

execute_query(Query, State) ->
    AddedArgs = [
        ?FILTER_TYPES_TO_SQL(?FILTER_TYPES, State#state.types),
        State#state.low_block,
        State#state.high_block,
        max(0, ?TXN_LIST_LIMIT - length(State#state.results))
    ],
    {ok, _, Results} = ?PREPARED_QUERY(Query, State#state.args ++ AddedArgs),
    State#state{results = State#state.results ++ Results}.

execute_rem_query(_Query, _HighBlock, undefined, State) ->
    State;
execute_rem_query(Query, HighBlock, TxnHash, State) ->
    AddedArgs = [
        ?FILTER_TYPES_TO_SQL(?FILTER_TYPES, State#state.types),
        HighBlock,
        TxnHash,
        max(0, ?TXN_LIST_LIMIT - length(State#state.results))
    ],
    {ok, _, Results} = ?PREPARED_QUERY(Query, State#state.args ++ AddedArgs),
    State#state{results = State#state.results ++ Results}.

get_txn_list(Args, {MinQuery, Query, _RemQuery}, [{cursor, undefined}, {filter_types, Types}]) ->
    {ok, #{height := CurrentBlock}} = bh_route_blocks:get_block_height(),
    %% High block is exclusive so start past the tip
    HighBlock = CurrentBlock + 1,
    case ?PREPARED_QUERY(MinQuery, Args) of
        {ok, _, [{MinBlock}]} ->
            State = #state{
                high_block = HighBlock,
                %% Aim for block alignment
                low_block = calc_low_block(HighBlock, MinBlock),
                min_block = MinBlock,
                args = Args,
                types = Types
            },
            mk_txn_list_result(execute_query(Query, State));
        _ ->
            throw(?RESPONSE_404)
    end;
get_txn_list(Args, {_MinQuery, Query, RemQuery}, [{cursor, Cursor}, {filter_types, _}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok,
            C = #{
                <<"block">> := HighBlock,
                <<"min_block">> := MinBlock
            }} ->
            State0 = #state{
                high_block = HighBlock,
                anchor_block = maps:get(<<"anchor_block">>, C, undefined),
                types = maps:get(<<"types">>, C, undefined),
                min_block = MinBlock,
                low_block = calc_low_block(HighBlock, MinBlock),
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
            %% and start growing from there
            mk_txn_list_result(grow_txn_list(Query, execute_query(Query, State1)));
        _ ->
            {error, badarg}
    end.

get_txn_count(Args, Query, [{filter_types, Types}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(Query, Args ++ [?FILTER_TYPES_TO_SQL(?FILTER_TYPES, Types)]),
    InitCounts = [{K, 0} || K <- ?FILTER_TYPES_TO_LIST(?FILTER_TYPES, Types)],
    {ok,
        lists:foldl(
            fun({Key, Count}, Acc) ->
                maps:put(Key, Count, Acc)
            end,
            maps:from_list(InitCounts),
            Results
        )}.

mk_txn_list_result(State = #state{results = Results}) when length(Results) >= ?TXN_LIST_LIMIT ->
    {Trimmed, _Remainder} = lists:split(?TXN_LIST_LIMIT, Results),
    {Height, _Time, Hash, _Type, _Fields} = lists:last(Trimmed),
    {ok, txn_list_to_json(Trimmed), mk_txn_list_cursor(Height, Hash, State)};
mk_txn_list_result(State = #state{results = Results}) ->
    {ok, txn_list_to_json(Results), mk_txn_list_cursor(State#state.low_block, undefined, State)}.

mk_txn_list_cursor(MinBlock, undefined, #state{min_block = MinBlock}) ->
    undefined;
mk_txn_list_cursor(BeforeBlock, BeforeAddr, State = #state{}) ->
    %% Check if we didn't have an anchor block before and we've reached an anchor point
    AnchorBlock =
        case
            (State#state.anchor_block == undefined) and
                ((BeforeBlock rem ?TXN_LIST_BLOCK_ALIGN) == 0)
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
            {txn, BeforeAddr},
            {anchor_block, AnchorBlock},
            {types, State#state.types},
            {min_block, State#state.min_block}
        ]
    ).

get_txn_list_cache_time({ok, _, undefined}) ->
    %% Undefined cursor means we're at block 1. We assume a cursor got us here
    %% so cache forever (is this assumption too much?)
    infinity;
get_txn_list_cache_time({ok, _, Cursor = #{block := BeforeBlock}}) ->
    %% If we have an anchor block we have aligned at some point -> cache forever.
    %% therwise we're still trying to get to an anchor block. If the anchor
    %% block was just found (i.e. equal to BeforeBlock) we can't cache long
    case maps:get(anchor_block, Cursor, undefined) of
        undefined -> block_time;
        BeforeBlock -> block_time;
        _AnchorBlock -> infinity
    end;
get_txn_list_cache_time(_) ->
    never.

%%
%% to_jaon
%%

txn_list_to_json(Results) ->
    lists:map(fun txn_to_json/1, Results).

txn_to_json({Height, Time, Hash, Type, Fields}) ->
    Json = txn_to_json({Type, Fields}),
    Json#{
        <<"type">> => Type,
        <<"hash">> => Hash,
        <<"height">> => Height,
        <<"time">> => Time
    };
txn_to_json({<<"poc_request_v1">>, #{<<"location">> := Location} = Fields}) ->
    ?INSERT_LAT_LON(Location, Fields);
txn_to_json(
    {<<"poc_receipts_v1">>,
        #{<<"challenger_location">> := ChallengerLoc, <<"path">> := Path} = Fields}
) ->
    %% update witnesses to include location_hex at res8
    WitnessLocationHex = fun(PathElem) ->
        maps:update_with(
            <<"witnesses">>,
            fun(Witnesses) ->
                lists:map(
                    fun(Witness = #{<<"location">> := WitnessLoc}) ->
                        ?INSERT_LOCATION_HEX(WitnessLoc, Witness)
                    end,
                    Witnesses
                )
            end,
            PathElem
        )
    end,
    %% update challengee lat/lon in a path element
    LatLon = fun(PathElem = #{<<"challengee_location">> := ChallengeeLoc}) ->
        ?INSERT_LAT_LON(
            ChallengeeLoc,
            {<<"challengee_lat">>, <<"challengee_lon">>},
            PathElem
        )
    end,
    %% Insert geo code infomration for a challengee location in a path element
    Geocode = fun(PathElem = #{<<"challengee_location">> := ChallengeeLoc}) ->
        case ?PREPARED_QUERY(?S_LOC, [ChallengeeLoc]) of
            {ok, _, [Result]} ->
                PathElem#{<<"geocode">> => bh_route_hotspots:to_geo_json(Result)};
            _ ->
                PathElem
        end
    end,
    %% Insert lat lon for all path entries. Insert geocode information
    %% for just the first challengee
    NewPath = lists:map(
        fun
            ({1, PathElem}) ->
                LatLon(Geocode(WitnessLocationHex(PathElem)));
            ({_, PathElem}) ->
                LatLon(WitnessLocationHex(PathElem))
        end,
        lists:zip(lists:seq(1, length(Path)), Path)
    ),
    ?INSERT_LAT_LON(ChallengerLoc, {<<"challenger_lat">>, <<"challenger_lon">>}, Fields#{
        <<"path">> => NewPath
    });
txn_to_json({<<"gen_gateway_v1">>, Fields}) ->
    txn_to_json({<<"add_gateway_v1">>, Fields});
txn_to_json({<<"add_gateway_v1">>, Fields}) ->
    Fields#{
        <<"payer">> => maps:get(<<"payer">>, Fields, undefined),
        <<"fee">> => maps:get(<<"fee">>, Fields, 0),
        <<"staking_fee">> => maps:get(<<"staking_fee">>, Fields, 1)
    };
txn_to_json(
    {<<"assert_location_v1">>,
        #{
            <<"location">> := Location
        } = Fields}
) ->
    ?INSERT_LAT_LON(Location, Fields);
txn_to_json(
    {<<"assert_location_v2">>,
        #{
            <<"location">> := Location
        } = Fields}
) ->
    ?INSERT_LAT_LON(Location, Fields);
txn_to_json(
    {<<"token_burn_v1">>,
        #{
            <<"memo">> := Memo
        } = Fields}
) ->
    %% Adjust for some memos being base64 and others still integers
    MemoStr =
        case is_integer(Memo) of
            true -> base64:encode(<<Memo:64/unsigned-little-integer>>);
            _ -> Memo
        end,
    Fields#{
        <<"memo">> => MemoStr
    };
txn_to_json({_, Fields}) ->
    Fields.

%% txn_to_json({Type, _Fields}) ->
%%     lager:error("Unhandled transaction type ~p", [Type]),
%%     error({unhandled_txn_type, Type}).
