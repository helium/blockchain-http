-module(bh_route_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_txn/1,
         get_account_activity_list/2,
         txn_to_json/1,
         txn_list_to_json/1,
         filter_types/1]).

-define(S_TXN, "txn").
-define(S_ACTOR_TXN_LIST, "actor_txn_list").
-define(S_ACTOR_ACTIVITY_LIST, "actor_activity_list").
-define(S_ACTOR_ACTIVITY_LIST_BEFORE, "actor_activity_list_before").

-define(SELECT_TXN_FIELDS(F), ["select t.block, t.time, t.hash, t.type, ", (F), " "]).
-define(SELECT_TXN_BASE, [?SELECT_TXN_FIELDS("t.fields"), "from transactions t "]).

-define(SELECT_ACTOR_ACTIVITY_BASE,
        [?SELECT_TXN_FIELDS("txn_filter_account_activity(t.actor, t.type, t.fields) as fields"),
         "from (select tr.*, a.actor ",
         "from transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash ",
         %% Select the actor address from the account_ledger to ensure
         %% it is an actual existing account and not some other actor.
         "where a.actor = (select address from account_ledger where address = $1) ",
         "and tr.type = ANY($2) order by tr.block desc, tr.hash) as t "
         ]).
-define(ACTOR_ACTIVITY_LIST_LIMIT, 50).


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
                             "where t.hash = $1"],
                            []),

    {ok, S2} = epgsql:parse(Conn, ?S_ACTOR_ACTIVITY_LIST,
                            [?SELECT_ACTOR_ACTIVITY_BASE,
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_ACTOR_ACTIVITY_LIST_BEFORE,
                            [?SELECT_ACTOR_ACTIVITY_BASE,
                             "where (t.block = $3 and t.hash > $4) or t.block < $3 ",
                             "limit ", integer_to_list(?ACTOR_ACTIVITY_LIST_LIMIT)
                            ],
                            []),

    #{?S_TXN => S1,
      ?S_ACTOR_ACTIVITY_LIST => S2,
      ?S_ACTOR_ACTIVITY_LIST_BEFORE => S3
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


get_account_activity_list(Account, [{cursor, undefined}, {filter_types, Types}]) ->
    Result = ?PREPARED_QUERY(?S_ACTOR_ACTIVITY_LIST, [Account, filter_types(Types)]),
    mk_account_activity_list_from_result(Account, Types, Result);
get_account_activity_list(Account, [{cursor, Cursor}, {filter_types, _}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, C=#{ <<"hash">> := Hash,
                  <<"block">> := Block}} ->
            Types = maps:get(<<"types">>, C, undefined),
            Result = ?PREPARED_QUERY(?S_ACTOR_ACTIVITY_LIST_BEFORE, [Account, filter_types(Types), Block, Hash]),
            mk_account_activity_list_from_result(Account, Types, Result);
        _ ->
            {error, badarg}
    end.


mk_account_activity_list_from_result(_Account, _Types, {ok, _, []}) ->
    {error, not_found};
mk_account_activity_list_from_result(Account, Types, {ok, _, Results}) ->
    {ok, txn_list_to_json(Results), mk_account_activiyt_cursor(Account, Types, Results)}.


mk_account_activiyt_cursor(Account, Types, Results) ->
    case length(Results) < ?ACTOR_ACTIVITY_LIST_LIMIT of
        true -> undefined;
        false ->
            {Height, _Time, Hash, _Type, _Fields} = lists:last(Results),
            Cursor0 = #{ hash => Hash,
                         account => Account,
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
