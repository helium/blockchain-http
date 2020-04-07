-module(bh_route_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_txn/1,
         get_block_txn_list/2,
         get_activity_list/2,
         txn_to_json/1,
         txn_list_to_json/1,
         filter_types/1]).

-define(S_TXN, "txn").
-define(S_ACTOR_TXN_LIST, "actor_txn_list").
-define(S_ACCOUNT_ACTIVITY_LIST, "account_activity_list").
-define(S_ACCOUNT_ACTIVITY_LIST_BEFORE, "account_activity_list_before").
-define(S_HOTSPOT_ACTIVITY_LIST, "hotspot_activity_list").
-define(S_HOTSPOT_ACTIVITY_LIST_BEFORE, "hotspot_activity_list_before").
-define(S_BLOCK_HEIGHT_TXN_LIST, "block_height_txn_list").
-define(S_BLOCK_HEIGHT_TXN_LIST_BEFORE, "block_height_txn_list_before").
-define(S_BLOCK_HASH_TXN_LIST, "block_hash_txn_list_list").
-define(S_BLOCK_HASH_TXN_LIST_BEFORE, "block_hash_txn_list_before").

-define(SELECT_TXN_FIELDS(F), ["select t.block, t.time, t.hash, t.type, ", (F), " "]).
-define(SELECT_TXN_BASE, ?SELECT_TXN_FIELDS("t.fields")).

-define(SELECT_ACTOR_ACTIVITY_BASE(E),
        [?SELECT_TXN_FIELDS("txn_filter_actor_activity(t.actor, t.type, t.fields) as fields"),
         "from (select tr.*, a.actor ",
         "from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash ",
         " where a.actor = $1 ", (E),
         " and tr.type = ANY($2) order by tr.block desc, tr.hash) as t "
        ]).

-define(SELECT_ACCOUNT_ACTIVITY_BASE,
        ?SELECT_ACTOR_ACTIVITY_BASE(%% For account activity we limit
                                    %% the actor roles to just a
                                    %% few.
                                    "and a.actor_role in ('payer', 'payee', 'owner')")).
-define(SELECT_HOTSPOT_ACTIVITY_BASE,
        ?SELECT_ACTOR_ACTIVITY_BASE(%% Filter out gateway roles that
                                    %% should be in accounts
                                    "and a.actor_role not in ('payer', 'payee', 'owner')")).

-define(SELECT_BLOCK_HEIGHT_TXN_LIST_BASE,
        [?SELECT_TXN_BASE, "from (select * from transactions where block = $1 order by hash) t "]).

-define(SELECT_BLOCK_HASH_TXN_LIST_BASE,
        [?SELECT_TXN_BASE, "from (select * from transactions where block = (select height from blocks where block_hash = $1) order by hash) t "]).

-define(ACTOR_ACTIVITY_LIST_LIMIT, 50).
-define(BLOCK_TXN_LIST_LIMIT, 50).


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

    {ok, S2} = epgsql:parse(Conn, ?S_ACCOUNT_ACTIVITY_LIST,

                            [?SELECT_ACCOUNT_ACTIVITY_BASE,
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_ACCOUNT_ACTIVITY_LIST_BEFORE,
                            [?SELECT_ACCOUNT_ACTIVITY_BASE,
                             "where (t.block = $3 and t.hash > $4) or t.block < $3 ",
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_HOTSPOT_ACTIVITY_LIST,
                            [?SELECT_HOTSPOT_ACTIVITY_BASE,
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),


    {ok, S5} = epgsql:parse(Conn, ?S_HOTSPOT_ACTIVITY_LIST_BEFORE,
                            [?SELECT_HOTSPOT_ACTIVITY_BASE,
                             "where (t.block = $3 and t.hash > $4) or t.block < $3 ",
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),

    {ok, S6} = epgsql:parse(Conn, ?S_BLOCK_HEIGHT_TXN_LIST,
                            [?SELECT_BLOCK_HEIGHT_TXN_LIST_BASE,
                             "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)
                            ],
                            []),

    {ok, S7} = epgsql:parse(Conn, ?S_BLOCK_HEIGHT_TXN_LIST_BEFORE,
                            [?SELECT_BLOCK_HEIGHT_TXN_LIST_BASE,
                             "where t.hash > $2",
                             "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)
                            ],
                            []),

    {ok, S8} = epgsql:parse(Conn, ?S_BLOCK_HASH_TXN_LIST,
                            [?SELECT_BLOCK_HASH_TXN_LIST_BASE,
                             "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)
                            ],
                            []),

    {ok, S9} = epgsql:parse(Conn, ?S_BLOCK_HASH_TXN_LIST_BEFORE,
                            [?SELECT_BLOCK_HASH_TXN_LIST_BASE,
                             "where t.hash > $2",
                             "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)
                            ],
                            []),



    #{?S_TXN => S1,
      ?S_ACCOUNT_ACTIVITY_LIST => S2,
      ?S_ACCOUNT_ACTIVITY_LIST_BEFORE => S3,
      ?S_HOTSPOT_ACTIVITY_LIST => S4,
      ?S_HOTSPOT_ACTIVITY_LIST_BEFORE => S5,
      ?S_BLOCK_HEIGHT_TXN_LIST => S6,
      ?S_BLOCK_HEIGHT_TXN_LIST_BEFORE => S7,
      ?S_BLOCK_HASH_TXN_LIST => S8,
      ?S_BLOCK_HASH_TXN_LIST_BEFORE => S9
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



%%
%% Block Transctions
%%

get_block_txn_list({height, Height}, Args) ->
    get_block_txn_list(Height, {?S_BLOCK_HEIGHT_TXN_LIST, ?S_BLOCK_HEIGHT_TXN_LIST_BEFORE}, Args);
get_block_txn_list({hash, Hash}, Args) ->
    get_block_txn_list(Hash, {?S_BLOCK_HASH_TXN_LIST, ?S_BLOCK_HASH_TXN_LIST_BEFORE}, Args).

get_block_txn_list(Block, {StartQuery, _CursorQuery}, [{cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(StartQuery, [Block]),
    mk_txn_list_from_result(Result);
get_block_txn_list(Block, {_StartQuery, CursorQuery}, [{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"hash">> := Hash }} ->
            Result = ?PREPARED_QUERY(CursorQuery, [Block, Hash]),
            mk_txn_list_from_result(Result)
    end.

mk_txn_list_from_result({ok, _, Results}) ->
    {ok, txn_list_to_json(Results), mk_txn_list_cursor(Results)}.

mk_txn_list_cursor(Results) ->
    case length(Results) < ?BLOCK_TXN_LIST_LIMIT of
        true -> undefined;
        false ->
            {_Height, _Time, Hash, _Type, _Fields} = lists:last(Results),
            #{ hash => Hash}
    end.


%%
%% Activity
%%

get_activity_list({account, Account}, Args) ->
    get_activity_list(Account, {?S_ACCOUNT_ACTIVITY_LIST, ?S_ACCOUNT_ACTIVITY_LIST_BEFORE}, Args);
get_activity_list({hotspot, Address}, Args) ->
    get_activity_list(Address, {?S_HOTSPOT_ACTIVITY_LIST, ?S_HOTSPOT_ACTIVITY_LIST_BEFORE}, Args).


%%
%% Common Activity
%%

get_activity_list(Actor, {StartQuery, _CursorQuery}, [{cursor, undefined}, {filter_types, Types}]) ->
    Result = ?PREPARED_QUERY(StartQuery, [Actor, filter_types(Types)]),
    mk_activity_list_from_result(Types, Result);
get_activity_list(Actor, {_StartQuery, CursorQuery}, [{cursor, Cursor}, {filter_types, _}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, C=#{ <<"hash">> := Hash,
                  <<"block">> := Block}} ->
            Types = maps:get(<<"types">>, C, undefined),
            Result = ?PREPARED_QUERY(CursorQuery, [Actor, filter_types(Types), Block, Hash]),
            mk_activity_list_from_result(Types, Result);
        _ ->
            {error, badarg}
    end.

mk_activity_list_from_result(Types, {ok, _, Results}) ->
    {ok, txn_list_to_json(Results), mk_activity_cursor(Types, Results)}.


mk_activity_cursor(Types, Results) ->
    case length(Results) < ?ACTOR_ACTIVITY_LIST_LIMIT of
        true -> undefined;
        false ->
            {Height, _Time, Hash, _Type, _Fields} = lists:last(Results),
            Cursor0 = #{ hash => Hash,
                         block => Height},
            case Types of
                undefined -> Cursor0;
                _ -> Cursor0#{ types => Types}
            end
    end.


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
