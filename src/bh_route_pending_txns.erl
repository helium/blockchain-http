-module(bh_route_pending_txns).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_pending_txn_list/0,
         get_pending_txn/1,
         get_pending_txn_list/3]).


-define(S_PENDING_TXN_LIST_BEFORE, "pending_txn_list_before").
-define(S_PENDING_TXN, "pending_txn").
-define(S_INSERT_PENDING_TXN, "insert_pending_txn").

-define(SELECT_PENDING_TXN_BASE, "select t.created_at, t.updated_at, t.created_block, t.hash, t.status, t.failed_reason from pending_transactions t ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_PENDING_TXN_LIST_BEFORE,
                           ?SELECT_PENDING_TXN_BASE "where t.created_at < $2 and t.status = $1 order by created_at DESC limit $3", []),

    {ok, _} = epgsql:parse(Conn, ?S_PENDING_TXN,
                           ?SELECT_PENDING_TXN_BASE "where hash = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_INSERT_PENDING_TXN,
                           "insert into pending_transactions (hash, created_block, type, nonce, status, fields) values ($1, $2, $3, $4, $5, $6)", []),

    ok.

handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_pending_txn(elli_request:uri_decode(TxnHash)));

handle(_, _, _Req) ->
    ?RESPONSE_404.

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

pending_txn_to_json({CreatedAt, UpdatedAt, CreatedBlock, Hash, Status, FailedReason}) ->
    #{
      <<"created_at">> => iso8601:format(CreatedAt),
      <<"updated_at">> => iso8601:format(UpdatedAt),
      <<"created_block">> => CreatedBlock,
      <<"hash">> => Hash,
      <<"status">> => Status,
      <<"failed_reason">> => FailedReason
     }.
