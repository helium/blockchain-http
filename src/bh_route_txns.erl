-module(bh_route_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_txn/1,
         get_txn_list/2,
         get_actor_txn_list/3,
         get_activity_list/2,
         txn_to_json/1,
         txn_list_to_json/1,
         filter_types/1]).

-define(S_TXN, "txn").
-define(S_TXN_LIST, "txn_list").
-define(S_ACTOR_TXN_LIST, "actor_txn_list").
-define(S_OWNED_ACTOR_TXN_LIST, "owned_actor_txn_list").
-define(S_ACCOUNT_ACTIVITY_LIST, "account_activity_list").
-define(S_ACCOUNT_ACTIVITY_LIST_BEFORE, "account_activity_list_before").
-define(S_HOTSPOT_ACTIVITY_LIST, "hotspot_activity_list").
-define(S_HOTSPOT_ACTIVITY_LIST_BEFORE, "hotspot_activity_list_before").

-define(SELECT_TXN_LIST,
       [?SELECT_TXN_BASE,
        "from transactions t ",
        "where t.type = ANY($1)",
        " and block >= $2 and block < $3"
        " order by t.block desc, t.hash"
       ]).

-define(SELECT_ACTOR_TXN_LIST_BASE(F, E),
        [?SELECT_TXN_FIELDS(F),
         "from (select tr.*, a.actor ",
         "from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash ",
         " where a.block >= $3 and a.block < $4 and a.actor = $1 ", (E),
         " and tr.type = ANY($2) order by tr.block desc) as t "
        ]).

-define(SELECT_OWNED_ACTOR_TXN_LIST_BASE(F, E),
        [?SELECT_TXN_FIELDS(F),
         "from (select tr.*, a.actor ",
         "from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash ",
         " where a.block >= $3 and a.block < $4",
         " and a.actor in (select address from gateway_ledger where owner = $1) ", (E),
         " and tr.type = ANY($2) order by tr.block desc) as t "
        ]).

-define(SELECT_ACTOR_TXN_LIST, ?SELECT_ACTOR_TXN_LIST_BASE("t.fields", "")).
-define(SELECT_OWNED_ACTOR_TXN_LIST, ?SELECT_OWNED_ACTOR_TXN_LIST_BASE("t.fields", "")).

-define(SELECT_ACCOUNT_ACTIVITY_LIST,
        %% For account activity we limit the actor roles to just a few.
        ?SELECT_ACTOR_TXN_LIST_BASE(
           "txn_filter_actor_activity(t.actor, t.type, t.fields) as fields",
           "and a.actor_role in ('payer', 'payee', 'owner')")).

-define(SELECT_HOTSPOT_ACTIVITY_LIST,
        %% Filter out gateway roles that should be in accounts
        ?SELECT_ACTOR_TXN_LIST_BASE(
           "txn_filter_actor_activity(t.actor, t.type, t.fields) as fields",
           "and a.actor_role not in ('payer', 'payee', 'owner')")).

-define(FILTER_TYPES,
        [<<"coinbase_v1">>,
         <<"security_coinbase_v1">>,
         <<"oui_v1">>,
         <<"gen_gateway_v1">>,
         <<"routing_v1">>,
         <<"payment_v1">>,
         <<"security_exchange_v1">>,
         <<"consensus_group_v1">>,
         <<"add_gateway_v1">>,
         <<"assert_location_v1">>,
         <<"create_htlc_v1">>,
         <<"redeem_htlc_v1">>,
         <<"poc_request_v1">>,
         <<"poc_receipts_v1">>,
         <<"vars_v1">>,
         <<"rewards_v1">>,
         <<"token_burn_v1">>,
         <<"dc_coinbase_v1">>,
         <<"token_burn_exchange_rate_v1">>,
         <<"payment_v2">>]).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_TXN,
                            [?SELECT_TXN_BASE,
                             "from transactions t ",
                             "where t.hash = $1"],
                            []),

    {ok, S2} = epgsql:parse(Conn, ?S_TXN_LIST, ?SELECT_TXN_LIST,
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_ACTOR_TXN_LIST, ?SELECT_ACTOR_TXN_LIST,
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_OWNED_ACTOR_TXN_LIST, ?SELECT_OWNED_ACTOR_TXN_LIST,
                            []),

    {ok, S5} = epgsql:parse(Conn, ?S_ACCOUNT_ACTIVITY_LIST, ?SELECT_ACCOUNT_ACTIVITY_LIST,
                            []),

    {ok, S6} = epgsql:parse(Conn, ?S_HOTSPOT_ACTIVITY_LIST, ?SELECT_HOTSPOT_ACTIVITY_LIST,
                            []),


    #{
      ?S_TXN => S1,
      ?S_TXN_LIST => S2,
      ?S_ACTOR_TXN_LIST => S3,
      ?S_OWNED_ACTOR_TXN_LIST => S4,
      ?S_ACCOUNT_ACTIVITY_LIST => S5,
      ?S_HOTSPOT_ACTIVITY_LIST => S6
     }.

handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_txn(TxnHash), infinity);

handle(_, _, _Req) ->
    ?RESPONSE_404.

-spec get_txn(Key::binary()) -> {ok, jiffy:json_object()} | {error, term()}.
get_txn(Key) ->
    case ?PREPARED_QUERY(?S_TXN, [Key]) of
        {ok, _, [Result]} ->
            {ok, txn_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_txn_list(BlockLimit, Args=[{cursor, _}, {filter_types, _}]) ->
    get_txn_list([], ?S_TXN_LIST, BlockLimit, Args).

get_actor_txn_list({actor, Address}, BlockLimit, Args=[{cursor, _}, {filter_types, _}]) ->
    get_txn_list([Address], ?S_ACTOR_TXN_LIST, BlockLimit, Args);
get_actor_txn_list({owned, Address}, BlockLimit, Args=[{cursor, _}, {filter_types, _}]) ->
    get_txn_list([Address], ?S_OWNED_ACTOR_TXN_LIST, BlockLimit, Args).

get_activity_list({account, Account}, Args) ->
    get_txn_list([Account], ?S_ACCOUNT_ACTIVITY_LIST, ?ACTIVITY_LIST_BLOCK_LIMIT, Args);
get_activity_list({hotspot, Address}, Args) ->
    get_txn_list([Address], ?S_HOTSPOT_ACTIVITY_LIST, ?ACTIVITY_LIST_BLOCK_LIMIT, Args).

get_txn_list(Args, Query, BlockLimit, [{cursor, undefined}, {filter_types, Types}]) ->
    {ok, #{<<"height">> := CurrentBlock}} = bh_route_blocks:get_block_height(),
    %% High block is exclusive so start past the tip
    HighBlock = CurrentBlock + 1,
    %% Ensure block alignment for the lower end
    LowBlock = HighBlock - (HighBlock rem BlockLimit),
    Result = ?PREPARED_QUERY(Query, Args ++ [filter_types(Types), LowBlock, HighBlock]),
    mk_txn_list_from_result({LowBlock, HighBlock}, BlockLimit, Types, Result);
get_txn_list(Args, Query, BlockLimit, [{cursor, Cursor}, {filter_types, _}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, C=#{ <<"block">> := Before }} ->
            Types = maps:get(<<"types">>, C, undefined),
            %% Before was the low block and is now the high
            %% block. Bound at block 2 since high block is exclusive
            HighBlock = max(2, Before - (Before rem BlockLimit)),
            %% Low block is inclusive, lower bound at block 1
            LowBlock = max(1, HighBlock - BlockLimit),
            Result = ?PREPARED_QUERY(Query, Args ++ [filter_types(Types), LowBlock, HighBlock]),
            mk_txn_list_from_result({LowBlock, HighBlock}, BlockLimit, Types, Result);
        _ ->
            {error, badarg}
    end.



mk_txn_list_from_result({LowBlock, HighBlock}, BlockLimit, Types, {ok, _, Results}) ->
    {ok,
     txn_list_to_json(Results),
     mk_txn_list_cursor(LowBlock, BlockLimit, Types),
     mk_txn_list_meta({LowBlock, HighBlock})}.

mk_txn_list_cursor(1, _BlockLimit, _Types) ->
    undefined;
mk_txn_list_cursor(LowBlock, BlockLimit, Types) ->
    Cursor0 = #{
                 block => LowBlock,
                %% include the block range size to have it be used as
                %% part of any cache key strategy. This not actually
                %% used as part of queries.
                range => BlockLimit
               },
    case Types of
        undefined -> Cursor0;
        _ -> Cursor0#{ types => Types}
    end.

mk_txn_list_meta({LowBlock, HighBlock}) ->
    #{
       start_block => HighBlock,
       end_block => LowBlock
     }.



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

txn_to_json({<<"poc_request_v1">>,
             #{ <<"location">> := Location } = Fields}) ->
    ?INSERT_LAT_LON(Location, Fields);
txn_to_json({<<"poc_receipts_v1">>,
             #{ <<"challenger_loc">> := ChallengerLoc } = Fields}) ->
    ?INSERT_LAT_LON(ChallengerLoc, {<<"challenger_lat">>, <<"challenger_lon">>}, Fields);
txn_to_json({<<"gen_gateway_v1">>, Fields}) ->
    txn_to_json({<<"add_gateway_v1">>, Fields});
txn_to_json({<<"add_gateway_v1">>, Fields}) ->
    Fields#{
            <<"payer">> => maps:get(<<"payer">>, Fields, undefined),
            <<"fee">> => maps:get(<<"fee">>, Fields, 0),
            <<"staking_fee">> => maps:get(<<"staking_fee">>, Fields, 1)
           };
txn_to_json({<<"assert_location_v1">>,
             #{
               <<"location">> := Location
              } = Fields}) ->
    ?INSERT_LAT_LON(Location, Fields);
txn_to_json({<<"security_coinbase_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"dc_coinbase_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"consensus_group_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"vars_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"oui_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"rewards_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"payment_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"payment_v2">>, Fields}) ->
    Fields;
txn_to_json({<<"create_htlc_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"redeem_htlc_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"state_channel_open_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"state_channel_close_v1">>, Fields}) ->
    Fields.

%% txn_to_json({Type, _Fields}) ->
%%     lager:error("Unhandled transaction type ~p", [Type]),
%%     error({unhandled_txn_type, Type}).

-spec filter_types(undefined | [binary()] | binary()) -> iolist().
filter_types(undefined) ->
    filter_types(?FILTER_TYPES);
filter_types(Bin) when is_binary(Bin) ->
    SplitTypes = binary:split(Bin, <<",">>, [global]),
    Types = lists:filter(fun(T) -> lists:member(T, ?FILTER_TYPES) end, SplitTypes),
    filter_types(Types);
filter_types(Types) when is_list(Types) ->
    [<<"{">>, lists:join(<<",">>, Types), <<"}">>].
