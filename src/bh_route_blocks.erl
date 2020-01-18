-module(bh_route_blocks).

-behavior(bh_route_handler).

-include("bh_db_worker.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_block_list/2, get_block/1, get_block_txn_list/1]).

-define(S_BLOCK_LIST_BEFORE, "block_list_before").
-define(S_BLOCK_LIST, "block_list").
-define(S_BLOCK, "block").
-define(S_BLOCK_TXN_LIST, "block_txn_list").

prepare_conn(Conn) ->
    epgsql:parse(Conn, ?S_BLOCK_LIST_BEFORE,
                 "select height, time, block_hash, transaction_count from blocks where height <= $1 order by height DESC limit $2", []),
    epgsql:parse(Conn, ?S_BLOCK_LIST,
                 "select height, time, block_hash, transaction_count from blocks where height <= (select max(height) from blocks) order by height DESC limit $1", []),
    epgsql:parse(Conn, ?S_BLOCK,
                 "select height, time, block_hash, transaction_count from blocks where height = $1", []),
    epgsql:parse(Conn, ?S_BLOCK_TXN_LIST,
                 "select block, hash, type, fields from transactions where block = $1", []),
    ok.


-define(DEFAULT_LIMIT_ARG, <<"100">>).
-define(MAX_LIMIT, 1000).

handle('GET', [], Req) ->
    Before = binary_to_integer(elli_request:get_arg(<<"before">>, Req, <<"-1">>)),
    Limit = min(?MAX_LIMIT, binary_to_integer(elli_request:get_arg(<<"limit">>, Req, ?DEFAULT_LIMIT_ARG))),
    mk_response(get_block_list(Before, Limit));
handle('GET', [BlockId], _Req) ->
    mk_response(get_block(binary_to_integer(BlockId)));
handle('GET', [BlockId, <<"transactions">>], _Req) ->
    mk_response(get_block_txn_list(binary_to_integer(BlockId)));

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.


mk_response({ok, Json}) ->
    {ok,
     [{<<"Content-Type">>, <<"application/json; charset=utf-8">>}],
     jsone:encode(#{<<"data">> => Json}, [undefined_as_null])}.

get_block_list(Before, Limit) when Before =< 0 ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST, [Limit]),
    {ok, block_list_to_json(Results)};
get_block_list(Before, Limit) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST_BEFORE, [Before, Limit]),
    {ok, block_list_to_json(Results)}.

get_block(Height) ->
    case ?PREPARED_QUERY(?S_BLOCK, [Height]) of
        {ok, _, [Result]} ->
            {ok, block_to_json(Result)};
        _ ->
            {ok, undefined}
    end.

get_block_txn_list(Height) ->
    case ?EQUERY("select t.block, b.time, t.hash, t.type, t.fields from transactions t inner join blocks b on b.height = t.block where b.height = $1", [Height]) of
    %% case ?PREPARED_QUERY(?S_BLOCK_TXN_LIST, [Height]) of
        {ok, _, Result} ->
            {ok, txn_list_to_json(Result)};
        O ->
            lager:info("OTHER: ~p", [O]),
            {ok, []}
    end.

block_list_to_json(Results) ->
    lists:map(fun block_to_json/1, Results).

block_to_json({Height, Time, Hash, TxnCount}) ->
    #{
      <<"height">> => Height,
      <<"time">> => Time,
      <<"hash">> => Hash,
      <<"txns">> => TxnCount
     }.

txn_list_to_json(Results) ->
    lists:map(fun txn_to_json/1, Results).

txn_to_json({Height, Time, Hash, Type, Fields}) ->
    Json = txn_to_json({Type, Fields}),
    Json#{
          hash => Hash,
          time => Time,
          heiht => Height
         };

txn_to_json({<<"poc_request_v1">>,
             #{<<"fee">> := Fee,
               <<"onion_key_hash">> := Onion,
               <<"signature">> := Signature,
               <<"challenger">> := Challenger,
               <<"location">> := Location,
               <<"owner">> := Owner}}) ->
    lat_lon(Location,
            #{
              type => <<"poc_request">>,
              fee => Fee,
              onion => Onion,
              signature => Signature,
              challenger => Challenger,
              owner => Owner,
              location => Owner
             });
txn_to_json({<<"poc_receipts_v1">>,
             #{<<"fee">> := Fee,
               <<"onion_key_hash">> := Onion,
               <<"signature">> := Signature,
               <<"challenger">> := Challenger,
               <<"challenger_loc">> := ChallengerLoc,
               <<"challenger_owner">> := ChallengerOwner}}) ->
    lat_lon(ChallengerLoc, {challenger_lat, challenger_lon},
            #{
              type => <<"poc_receipts">>,
              fee => Fee,
              onion => Onion,
              signature => Signature,
              challenger => Challenger,
              challenger_owner => ChallengerOwner,
              location => ChallengerLoc
             });
txn_to_json({<<"gen_gateway_v1">>, Fields}) ->
    txn_to_json({<<"add_gateway_v1">>, Fields});
txn_to_json({<<"add_gateway_v1">>,
             #{
               <<"gateway">> := Gateway,
               <<"owner">> := Owner
              } = Fields}) ->
    #{
      type => <<"gateway">>,
      gateway => Gateway,
      owner => Owner,
      payer => maps:get(<<"payer">>, Fields, undefined),
      fee => maps:get(<<"fee">>, Fields, 0),
      staking_fee => maps:get(<<"staking_fee">>, Fields, 1)
     };
txn_to_json({<<"assert_location_v1">>,
             #{
               <<"location">> := Location
              } = Fields}) ->
    lat_lon(Location,
            Fields#{
                    type => <<"location">>
                   });
txn_to_json({<<"security_coinbase_v1">>, Fields}) ->
    Fields#{
             type => <<"security">>
           };
txn_to_json({<<"dc_coinbase_v1">>, Fields}) ->
    Fields#{
             type => <<"data_credit">>
           };
txn_to_json({<<"consensus_group_v1">>,
            #{
              <<"members">> := Members,
              <<"proof">> := Proof,
              <<"height">> := ElectionHeight,
              <<"delay">> := Delay
             }}) ->
    #{
      type => <<"election">>,
      members => Members,
      proof => Proof,
      election_height => ElectionHeight,
      delay => Delay
     };
txn_to_json({<<"vars_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"oui_v1">>, Fields}) ->
    Fields;
txn_to_json({<<"rewards_v1">>, Fields}) ->
    %% For some reason the old API doesn't include the reward
    %% participants in <block>/transactions. Include them here.
    Fields#{
      type => <<"rewards">>
     };
txn_to_json({<<"payment_v1">>, Fields}) ->
    Fields.

%% txn_to_json({Type, _Fields}) ->
%%     lager:error("Unhandled transaction type ~p", [Type]),
%%     error({unhandled_txn_type, Type}).


lat_lon(Location, Fields) ->
    lat_lon(Location, {lat, lng}, Fields).

lat_lon(undefined, _, Fields) ->
    Fields;
lat_lon(Location, {LatName, LonName}, Fields) when is_binary(Location) ->
    {Lat, Lon} = h3:to_geo(h3:from_string(binary_to_list(Location))),
    Fields#{
            LatName => Lat,
            LonName => Lon
           }.
