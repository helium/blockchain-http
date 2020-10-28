-module(bh_route_pending_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").
-include_lib("helium_proto/include/blockchain_txn_pb.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_pending_txn_list/2,
         insert_pending_txn/2]).


-define(S_ACTOR_PENDING_TXN_LIST, "actor_pending_txn_list").
-define(S_ACTOR_PENDING_TXN_LIST_BEFORE, "actor_pending_txn_list_before").
-define(S_PENDING_TXN_LIST, "pending_txn_list").
-define(S_PENDING_TXN_LIST_BEFORE, "pending_txn_list_before").
-define(S_INSERT_PENDING_TXN, "insert_pending_txn").

-define(SELECT_PENDING_TXN_FIELDS,
        "select t.created_at, t.updated_at, t.hash, t.type, t.status, t.failed_reason, t.fields ").

-define(SELECT_ACTOR_PENDING_TXN_LIST_BASE(E),
        [?SELECT_PENDING_TXN_FIELDS,
         "from pending_transaction_actors a inner join pending_transactions t on a.created_at = t.created_at ",
         "where a.actor = $1 ", (E), " ",
         "order by created_at desc ",
         "limit ", integer_to_list(?PENDING_TXN_LIST_LIMIT)
        ]).

-define(SELECT_PENDING_TXN_LIST_BASE(E),
        [?SELECT_PENDING_TXN_FIELDS,
         "from pending_transactions t ",
         "where hash = $1 ", (E), " ",
         "order by created_at desc ",
         "limit ", integer_to_list(?PENDING_TXN_LIST_LIMIT)
        ]).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_ACTOR_PENDING_TXN_LIST,
                            ?SELECT_ACTOR_PENDING_TXN_LIST_BASE(""),
                            []),

    {ok, S2} = epgsql:parse(Conn, ?S_ACTOR_PENDING_TXN_LIST_BEFORE,
                            ?SELECT_ACTOR_PENDING_TXN_LIST_BASE("and t.created_at < $2"),
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_PENDING_TXN_LIST,
                            ?SELECT_PENDING_TXN_LIST_BASE(""),
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_PENDING_TXN_LIST_BEFORE,
                            ?SELECT_PENDING_TXN_LIST_BASE("and t.created_at < $2"),
                            []),

    {ok, S5} = epgsql:parse(Conn, ?S_INSERT_PENDING_TXN,
                           ["insert into pending_transactions ",
                            "(hash, type, address, nonce, nonce_type, status, data) values ",
                            "($1, $2, $3, $4, $5, $6, $7)"],
                            []),

    #{?S_ACTOR_PENDING_TXN_LIST => S1,
      ?S_ACTOR_PENDING_TXN_LIST_BEFORE => S2,
      ?S_PENDING_TXN_LIST => S3,
      ?S_PENDING_TXN_LIST_BEFORE => S4,
      ?S_INSERT_PENDING_TXN => S5
     }.

handle('GET', [TxnHash], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_pending_txn_list({hash, TxnHash}, Args), block_time);
handle('POST', [], Req) ->
    #{ <<"txn">> := EncodedTxn } = jiffy:decode(elli_request:body(Req), [return_maps]),
    BinTxn = base64:decode(EncodedTxn),
    Txn = txn_unwrap(blockchain_txn_pb:decode_msg(BinTxn, blockchain_txn_pb)),
    Result = insert_pending_txn(Txn, BinTxn),
    ?MK_RESPONSE(Result, never);

handle(_, _, _Req) ->
    ?RESPONSE_404.

-type supported_txn() :: #blockchain_txn_oui_v1_pb{}
                       | #blockchain_txn_add_gateway_v1_pb{}
                       | #blockchain_txn_assert_location_v1_pb{}
                       | #blockchain_txn_payment_v1_pb{}
                       | #blockchain_txn_payment_v2_pb{}
                       | #blockchain_txn_create_htlc_v1_pb{}
                       | #blockchain_txn_redeem_htlc_v1_pb{}
                       | #blockchain_txn_price_oracle_v1_pb{}
                       | #blockchain_txn_token_burn_v1_pb{}
                       | #blockchain_txn_transfer_hotspot_v1_pb{}
                       | #blockchain_txn_security_exchange_v1_pb{}.

-type nonce_type() :: binary().

-spec insert_pending_txn(supported_txn(), binary()) -> {ok, jiffy:json_object()} | {error, term()}.
insert_pending_txn(#blockchain_txn_oui_v1_pb{owner=Owner}=Txn, Bin) ->
    %% There is no nonce type for that is useful to speculate values for
    insert_pending_txn(Txn, Owner, 0, <<"none">>, Bin);
insert_pending_txn(#blockchain_txn_add_gateway_v1_pb{gateway=GatewayAddress}=Txn, Bin) ->
    %% Adding a gateway uses the gateway nonce, even though it's
    %% expected to be 0 (a gateway can only be added once)
    insert_pending_txn(Txn, GatewayAddress, 0, <<"gateway">>, Bin);
insert_pending_txn(#blockchain_txn_assert_location_v1_pb{nonce=Nonce, gateway=GatewayAddress}=Txn, Bin) ->
    %% Asserting a location uses the gatway nonce
    insert_pending_txn(Txn, GatewayAddress, Nonce, <<"gateway">>, Bin);
insert_pending_txn(#blockchain_txn_payment_v1_pb{nonce=Nonce, payer=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, Nonce, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_payment_v2_pb{nonce=Nonce, payer=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, Nonce, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_create_htlc_v1_pb{nonce=Nonce, payer=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, Nonce, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_redeem_htlc_v1_pb{}=Txn, Bin) ->
    insert_pending_txn(Txn, undefined, 0, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_price_oracle_v1_pb{public_key=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, 0, <<"none">>, Bin);
insert_pending_txn(#blockchain_txn_security_exchange_v1_pb{nonce=Nonce, payer=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, Nonce, <<"security">>, Bin);
insert_pending_txn(#blockchain_txn_transfer_hotspot_v1_pb{buyer=Buyer, buyer_nonce=Nonce}=Txn, Bin) ->
    insert_pending_txn(Txn, Buyer, Nonce, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_token_burn_v1_pb{nonce=Nonce, payer=Address}=Txn, Bin) ->
    insert_pending_txn(Txn, Address, Nonce, <<"balance">>, Bin).


-spec insert_pending_txn(supported_txn(), libp2p_crypto:pubkey_bin()| undefined, non_neg_integer(), nonce_type(), binary()) -> {ok, jiffy:json_object()} | {error, term()}.
insert_pending_txn(Txn, Address, Nonce, NonceType, Bin) ->
    TxnHash = ?BIN_TO_B64(txn_hash(Txn)),
    B58Address = case Address of
                     undefined -> undefined;
                     _ -> ?BIN_TO_B58(Address)
                 end,
    Params = [
              TxnHash,
              txn_type(Txn),
              B58Address,
              Nonce,
              NonceType,
              <<"received">>,
              Bin
             ],
    case ?PREPARED_QUERY(?DB_RW_POOL, ?S_INSERT_PENDING_TXN, Params) of
        {ok, _} ->
            {ok, #{ <<"hash">> => TxnHash}};
        {error, {error, error, _, unique_violation, _, _}} ->
            {error, conflict}
    end.

get_pending_txn_list({hash, TxnHash}, Args=[{cursor, _}]) ->
    get_pending_txn_list([TxnHash], {?S_PENDING_TXN_LIST, ?S_PENDING_TXN_LIST_BEFORE}, Args);
get_pending_txn_list({actor, Actor}, Args=[{cursor, _}]) ->
    get_pending_txn_list([Actor], {?S_ACTOR_PENDING_TXN_LIST, ?S_ACTOR_PENDING_TXN_LIST_BEFORE}, Args).


get_pending_txn_list(Args, {InitQuery, _CursorQuery}, [{cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(InitQuery, Args),
    mk_pending_txn_list_from_result(Result);
get_pending_txn_list(Args, {_InitQuery, CursorQuery}, [{cursor, Cursor}]) ->
    try ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before">> := Before}} ->
            BeforeDate = iso8601:parse(Before),
            Result = ?PREPARED_QUERY(CursorQuery, Args ++ [BeforeDate]),
            mk_pending_txn_list_from_result(Result);
        _ ->
            {error, badarg}
    catch
        _:_ ->
            %% handle badarg thrown in bad date formats
            {error, badarg}
    end.

mk_pending_txn_list_from_result({ok, _, Results}) ->
    {ok, pending_txn_list_to_json(Results), mk_pending_txn_cursor(Results)}.


mk_pending_txn_cursor(Results) ->
    case length(Results) < ?PENDING_TXN_LIST_LIMIT of
        true -> undefined;
        false ->
            {CreatedAt, _UpdatedAt, _Hash, _Type, _Status, _FailedReason, _Fields} = lists:last(Results),
            #{ before => iso8601:format(CreatedAt)}
    end.


%%
%% to_jaon
%%

pending_txn_list_to_json(Results) ->
    lists:map(fun pending_txn_to_json/1, Results).

pending_txn_to_json({CreatedAt, UpdatedAt, Hash, Type, Status, FailedReason, Fields}) ->
    #{
      created_at => iso8601:format(CreatedAt),
      updated_at => iso8601:format(UpdatedAt),
      hash => Hash,
      type => Type,
      status => Status,
      failed_reason => FailedReason,
      txn => Fields
     }.

%%
%% txn decoders
%%

txn_unwrap(#blockchain_txn_pb{txn={bundle, #blockchain_txn_bundle_v1_pb{transactions=Txns} = Bundle}}) ->
    Bundle#blockchain_txn_bundle_v1_pb{transactions=lists:map(fun txn_unwrap/1, Txns)};
txn_unwrap(#blockchain_txn_pb{txn={_, Txn}}) ->
    Txn.


-define(TXN_SIG_HASH(T),
        txn_hash(#T{}=Txn) ->
               BaseTxn = Txn#T{signature = <<>>},
               EncodedTxn = T:encode_msg(BaseTxn),
               crypto:hash(sha256, EncodedTxn) ).

-define(TXN_SOP_HASH(T),
        txn_hash(#T{}=Txn) ->
               BaseTxn = Txn#T{owner_signature = <<>>, payer_signature = <<>>},
               EncodedTxn = T:encode_msg(BaseTxn),
               crypto:hash(sha256, EncodedTxn) ).

-define(TXN_SOG_HASH(T),
        txn_hash(#T{}=Txn) ->
               BaseTxn = Txn#T{owner_signature = <<>>, gateway_signature = <<>>},
               EncodedTxn = T:encode_msg(BaseTxn),
               crypto:hash(sha256, EncodedTxn) ).

-define(TXN_SSB_HASH(T),
        txn_hash(#T{}=Txn) ->
            BaseTxn = Txn#T{seller_signature = <<>>, buyer_signature = <<>>},
            EncodedTxn = T:encode_msg(BaseTxn),
            crypto:hash(sha256, EncodedTxn) ).

-define(TXN_TYPE(T, B),
        txn_type(#T{}) ->
               B).

?TXN_SOP_HASH(blockchain_txn_oui_v1_pb);
?TXN_SOG_HASH(blockchain_txn_add_gateway_v1_pb);
?TXN_SOG_HASH(blockchain_txn_assert_location_v1_pb);
?TXN_SIG_HASH(blockchain_txn_payment_v1_pb);
?TXN_SIG_HASH(blockchain_txn_payment_v2_pb);
?TXN_SIG_HASH(blockchain_txn_create_htlc_v1_pb);
?TXN_SIG_HASH(blockchain_txn_redeem_htlc_v1_pb);
?TXN_SIG_HASH(blockchain_txn_price_oracle_v1_pb);
?TXN_SIG_HASH(blockchain_txn_token_burn_v1_pb);
?TXN_SSB_HASH(blockchain_txn_transfer_hotspot_v1_pb);
?TXN_SIG_HASH(blockchain_txn_security_exchange_v1_pb).

?TXN_TYPE(blockchain_txn_oui_v1_pb, <<"oui_v1">>);
?TXN_TYPE(blockchain_txn_add_gateway_v1_pb, <<"add_gateway_v1">>);
?TXN_TYPE(blockchain_txn_assert_location_v1_pb, <<"assert_location_v1">>);
?TXN_TYPE(blockchain_txn_payment_v1_pb, <<"payment_v1">>);
?TXN_TYPE(blockchain_txn_payment_v2_pb, <<"payment_v2">>);
?TXN_TYPE(blockchain_txn_create_htlc_v1_pb, <<"create_htlc_v1">>);
?TXN_TYPE(blockchain_txn_redeem_htlc_v1_pb, <<"redeem_htlc_v1">>);
?TXN_TYPE(blockchain_txn_price_oracle_v1_pb, <<"price_oracle_v1">>);
?TXN_TYPE(blockchain_txn_token_burn_v1_pb, <<"token_burn_v1">>);
?TXN_TYPE(blockchain_txn_transfer_hotspot_v1_pb, <<"transfer_hotspot_v1">>);
?TXN_TYPE(blockchain_txn_security_exchange_v1_pb, <<"security_exchange_v1">>).
