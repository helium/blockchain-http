-module(bh_route_pending_txns).

-behavior(bh_route_handler).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_pending_txn/1]).


-define(S_PENDING_TXN, "pending_txn").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_PENDING_TXN,
                           "select created_at, updated_at, created_block, hash, status, failed_reason from pending_transactions where hash = $1", []),

    ok.

handle('GET', [TxnHash], _Req) ->
    ?MK_RESPONSE(get_pending_txn(TxnHash));

handle(_, _, _Req) ->
    ?RESPONSE_404.

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

pending_txn_to_json({CreatedAt, UpdatedAt, CreatedBlock, Hash, Status, FailedReason}) ->
    #{
      <<"created_at">> => iso8601:format(CreatedAt),
      <<"updated_at">> => iso8601:format(UpdatedAt),
      <<"created_block">> => CreatedBlock,
      <<"hash">> => Hash,
      <<"status">> => Status,
      <<"failed_reason">> => FailedReason
     }.
