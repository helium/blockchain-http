-module(bh_route_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_txn_list/1, get_txn/1,
         txn_to_json/1, txn_list_to_json/1]).


-define(S_TXN, "txn").
-define(S_TXN_LIST, "txn_list").
-define(S_TXN_LIST_BEFORE, "txn_list_before").
-define(S_ACTOR_TXN_LIST, "actor_txn_list").
-define(S_ACTOR_TXN_LIST_BEFORE, "actor_txn_list_before").

-define(SELECT_TXN_FIELDS, "select t.block, t.hash, t.type, t.fields ").
-define(SELECT_TXN_BASE, ?SELECT_TXN_FIELDS "from transactions t ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_TXN,
                          ?SELECT_TXN_BASE "where t.hash = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_TXN_LIST,
                           ?SELECT_TXN_BASE "order by block desc limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_TXN_LIST_BEFORE,
                           ?SELECT_TXN_BASE "where t.height < $1 order by block desc limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACTOR_TXN_LIST,
                           ?SELECT_TXN_FIELDS "from transaction_actors a inner join transactions on a.transaction_hash = t.hash where a.actor = $1 order by block desc limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACTOR_TXN_LIST_BEFORE,
                           ?SELECT_TXN_FIELDS "from transaction_actors a inner join transactions on a.transaction_hash = t.hash where a.actor = $1 and t.height < $2 order by block desc limit $3", []),

    ok.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([before, limit], Req),
    ?MK_RESPONSE(get_txn_list([{actor, udefined} | Args]));
handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_txn(TxnHash));

handle(_, _, _Req) ->
    ?RESPONSE_404.

-spec get_txn(Key::binary()) -> {ok, jsone:json_object()} | {error, term()}.
get_txn(Key) ->
    case ?PREPARED_QUERY(?S_TXN, [Key]) of
        {ok, _, [Result]} ->
            {ok, txn_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_txn_list([{actor, undefined}, {before, undefined}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_TXN_LIST, [Limit]),
    {ok, txn_list_to_json(Results)};
get_txn_list([{actor, undefined}, {before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_TXN_LIST_BEFORE, [Before, Limit]),
    {ok, txn_list_to_json(Results)};

get_txn_list([{actor, Actor}, {before, undefined}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACTOR_TXN_LIST, [Actor, Limit]),
    {ok, txn_list_to_json(Results)};
get_txn_list([{actor, Actor}, {before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACTOR_TXN_LIST_BEFORE, [Actor, Before, Limit]),
    {ok, txn_list_to_json(Results)}.


%%
%% to_jaon
%%

txn_list_to_json(Results) ->
    lists:map(fun txn_to_json/1, Results).

txn_to_json({Height, Hash, Type, Fields}) ->
    Json = txn_to_json({Type, Fields}),
    Json#{
          <<"type">> => Type,
          <<"hash">> => Hash,
          <<"height">> => Height
         };

txn_to_json({<<"poc_request_v1">>,
             #{<<"fee">> := Fee,
               <<"onion_key_hash">> := Onion,
               <<"signature">> := Signature,
               <<"challenger">> := Challenger,
               <<"location">> := Location,
               <<"owner">> := Owner}}) ->
    ?INSERT_LAT_LON(Location,
                    #{
                      <<"fee">> => Fee,
                      <<"onion">> => Onion,
                      <<"signature">> => Signature,
                      <<"challenger">> => Challenger,
                      <<"owner">> => Owner,
                      <<"location">> => Owner
                     });
txn_to_json({<<"poc_receipts_v1">>,
             #{<<"fee">> := Fee,
               <<"onion_key_hash">> := Onion,
               <<"signature">> := Signature,
               <<"challenger">> := Challenger,
               <<"challenger_loc">> := ChallengerLoc,
               <<"challenger_owner">> := ChallengerOwner}}) ->
    ?INSERT_LAT_LON(ChallengerLoc, {<<"challenger_lat">>, <<"challenger_lon">>},
                    #{
                      <<"fee">> => Fee,
                      <<"onion">> => Onion,
                      <<"signature">> => Signature,
                      <<"challenger">> => Challenger,
                      <<"challenger_owner">> => ChallengerOwner,
                      <<"location">> => ChallengerLoc
                     });
txn_to_json({<<"gen_gateway_v1">>, Fields}) ->
    txn_to_json({<<"add_gateway_v1">>, Fields});
txn_to_json({<<"add_gateway_v1">>,
             #{
               <<"gateway">> := Gateway,
               <<"owner">> := Owner
              } = Fields}) ->
    #{
      <<"gateway">> => Gateway,
      <<"owner">> => Owner,
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
txn_to_json({<<"consensus_group_v1">>,
            #{
              <<"members">> := Members,
              <<"proof">> := Proof,
              <<"height">> := ElectionHeight,
              <<"delay">> := Delay
             }}) ->
    #{
      <<"members">> => Members,
      <<"proof">> => Proof,
      <<"election_height">> => ElectionHeight,
      <<"delay">> => Delay
     };
txn_to_json({<<"vars_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"oui_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"rewards_v1">>, Fields}) ->
    %% For some reason the old API doesn't include the reward
    %% participants in <block>/transactions. Include them here.
    Fields;
txn_to_json({<<"payment_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"state_channel_open_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"state_channel_close_v1">>, Fields}) ->
    Fields.

%% txn_to_json({Type, _Fields}) ->
%%     lager:error("Unhandled transaction type ~p", [Type]),
%%     error({unhandled_txn_type, Type}).
