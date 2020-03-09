-module(bh_route_pending_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").
-include_lib("helium_proto/include/blockchain_txn_pb.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_pending_txn_list/0,
         get_pending_txn/1,
         get_pending_txn_list/3,
         insert_pending_txn/2]).


-define(S_PENDING_TXN_LIST_BEFORE, "pending_txn_list_before").
-define(S_PENDING_TXN, "pending_txn").
-define(S_INSERT_PENDING_TXN, "insert_pending_txn").

-define(SELECT_PENDING_TXN_BASE, "select t.created_at, t.updated_at, t.hash, t.status, t.failed_reason from pending_transactions t ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_PENDING_TXN_LIST_BEFORE,
                           ?SELECT_PENDING_TXN_BASE "where t.created_at < $2 and t.status = $1 order by created_at DESC limit $3", []),

    {ok, _} = epgsql:parse(Conn, ?S_PENDING_TXN,
                           ?SELECT_PENDING_TXN_BASE "where hash = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_INSERT_PENDING_TXN,
                           "insert into pending_transactions (hash, type, address, nonce, nonce_type, status, data) values ($1, $2, $3, $4, $5, $6, $7)", []),

    ok.

handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_pending_txn(TxnHash));
handle('POST', [], Req) ->
    #{ <<"txn">> := EncodedTxn } = jsone:decode(elli_request:body(Req)),
    BinTxn = base64:decode(EncodedTxn),
    Txn = txn_unwrap(blockchain_txn_pb:decode_msg(BinTxn, blockchain_txn_pb)),
    Result = insert_pending_txn(Txn, BinTxn),
    ?MK_RESPONSE(Result);

handle(_, _, _Req) ->
    ?RESPONSE_404.

-type supported_txn() :: #blockchain_txn_payment_v1_pb{}
                         | #blockchain_txn_payment_v2_pb{}.
-type nonce_type() :: binary().

-spec insert_pending_txn(supported_txn(), binary()) -> {ok, jsone:json_object()}.
insert_pending_txn(#blockchain_txn_payment_v1_pb{payer=Payer, nonce=Nonce }=Txn, Bin) ->
    insert_pending_txn(Txn, Payer, Nonce, <<"balance">>, Bin);
insert_pending_txn(#blockchain_txn_payment_v2_pb{payer=Payer, nonce=Nonce}=Txn, Bin) ->
    insert_pending_txn(Txn, Payer, Nonce, <<"balance">>, Bin).

-spec insert_pending_txn(supported_txn(), libp2p_crypto:pubkey_bin(), non_neg_integer(), nonce_type(), binary()) -> {ok, jsone:json_object()}.
insert_pending_txn(Txn, Address, Nonce, NonceType, Bin) ->
    TxnHash = ?BIN_TO_B64(txn_hash(Txn)),
    Params = [
              TxnHash,
              txn_type(Txn),
              ?BIN_TO_B58(Address),
              Nonce,
              NonceType,
              <<"received">>,
              Bin
             ],
    {ok, _} = ?PREPARED_QUERY(?DB_RW_POOL, ?S_INSERT_PENDING_TXN, Params),
    {ok, #{ <<"hash">> => TxnHash}}.


%% @equiv get_pending_txn_list(Status, Before, Limit)
get_pending_txn_list() ->
    get_pending_txn_list(pending, calendar:universal_time(), ?MAX_LIMIT).

-spec get_pending_txn_list(be_pending_txn:status(), calendar:datetime(), non_neg_integer()) -> {ok, jsone:json_array()}.
get_pending_txn_list(Status, Before, Limit) ->
    {ok, _, Results} =  ?PREPARED_QUERY(?S_PENDING_TXN_LIST_BEFORE, [Status, Before, Limit]),
    {ok, pending_txn_list_to_json(Results)}.

-spec get_pending_txn(Key::binary()) -> {ok, jsone:json_object()} | {error, term()}.
get_pending_txn(Key) ->
    case ?PREPARED_QUERY(?S_PENDING_TXN, [Key]) of
        {ok, _, [Result]} ->
            {ok, pending_txn_to_json(Result)};
        _ ->
            {error, not_found}
    end.

%%
%% to_jaon
%%

pending_txn_list_to_json(Results) ->
    lists:map(fun pending_txn_to_json/1, Results).

pending_txn_to_json({CreatedAt, UpdatedAt, Hash, Status, FailedReason}) ->
    #{
      <<"created_at">> => iso8601:format(CreatedAt),
      <<"updated_at">> => iso8601:format(UpdatedAt),
      <<"hash">> => Hash,
      <<"status">> => Status,
      <<"failed_reason">> => FailedReason
     }.

%%
%% txn decoders
%%

txn_unwrap(#blockchain_txn_pb{txn={bundle, #blockchain_txn_bundle_v1_pb{transactions=Txns} = Bundle}}) ->
    Bundle#blockchain_txn_bundle_v1_pb{transactions=lists:map(fun txn_unwrap/1, Txns)};
txn_unwrap(#blockchain_txn_pb{txn={_, Txn}}) ->
    Txn.

txn_hash(#blockchain_txn_payment_v1_pb{}=Txn) ->
    BaseTxn = Txn#blockchain_txn_payment_v1_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v1_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn);
txn_hash(#blockchain_txn_payment_v2_pb{}=Txn) ->
    BaseTxn = Txn#blockchain_txn_payment_v2_pb{signature = <<>>},
    EncodedTxn = blockchain_txn_payment_v2_pb:encode_msg(BaseTxn),
    crypto:hash(sha256, EncodedTxn).

txn_type(#blockchain_txn_payment_v1_pb{}) ->
    <<"payment_v1">>;
txn_type(#blockchain_txn_payment_v2_pb{}) ->
    <<"payment_v2">>.
