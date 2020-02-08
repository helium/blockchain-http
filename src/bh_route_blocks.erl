-module(bh_route_blocks).

-behavior(bh_route_handler).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_block_height/0,
         get_block_list/2,
         get_block/1,
         get_block_txn_list/1,
         get_block_by_hash/1,
         get_block_by_hash_txn_list/1]).

-define(S_BLOCK_HEIGHT, "block_height").
-define(S_BLOCK_LIST_BEFORE, "block_list_before").
-define(S_BLOCK_LIST, "block_list").
-define(S_BLOCK, "block").
-define(S_BLOCK_TXN_LIST, "block_txn_list").
-define(S_BLOCK_BY_HASH, "block_by_hash").
-define(S_BLOCK_BY_HASH_TXN_LIST, "block_by_hash_txn_list").

-define(SELECT_BLOCK_BASE, "select b.height, b.time, b.block_hash, b.transaction_count from blocks b ").
-define(SELECT_TXN_BASE, "select t.block, t.hash, t.type, t.fields from transactions t ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_HEIGHT,
                           "select max(height) from blocks", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_LIST_BEFORE,
                           ?SELECT_BLOCK_BASE "where b.height < $1 order by height DESC limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_LIST,
                           ?SELECT_BLOCK_BASE "where b.height <= (select max(height) from blocks) order by height DESC limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK,
                           ?SELECT_BLOCK_BASE "where b.height = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_BY_HASH,
                           ?SELECT_BLOCK_BASE "where block_hash = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_TXN_LIST,
                          ?SELECT_TXN_BASE "where t.block = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_BY_HASH_TXN_LIST,
                           ?SELECT_TXN_BASE "inner join blocks b on b.height = t.block where b.block_hash = $1", []),

    ok.


handle('GET', [], Req) ->
    Before = binary_to_integer(?GET_ARG_BEFORE(Req)),
    Limit = ?GET_ARG_LIMIT(Req),
    ?MK_RESPONSE(get_block_list(Before, Limit));
handle('GET', [<<"height">>], _Req) ->
    ?MK_RESPONSE(get_block_height());
handle('GET', [<<"hash">>, BlockHash], _Req) ->
    ?MK_RESPONSE(get_block_by_hash(elli_request:uri_decode(BlockHash)));
handle('GET', [<<"hash">>, BlockHash, <<"transactions">>], _Req) ->
    ?MK_RESPONSE(get_block_by_hash_txn_list(elli_request:uri_decode(BlockHash)));
handle('GET', [BlockId], _Req) ->
    ?MK_RESPONSE(get_block(binary_to_integer(BlockId)));
handle('GET', [BlockId, <<"transactions">>], _Req) ->
    ?MK_RESPONSE(get_block_txn_list(binary_to_integer(BlockId)));

handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.


get_block_list(Before, Limit) when Before =< 0 ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST, [Limit]),
    {ok, block_list_to_json(Results)};
get_block_list(Before, Limit) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST_BEFORE, [Before, Limit]),
    {ok, block_list_to_json(Results)}.

get_block_height() ->
    {ok, _, [{Height}]} = ?PREPARED_QUERY(?S_BLOCK_HEIGHT, []),
    {ok, #{<<"height">> => Height}}.

get_block(Height) ->
    case ?PREPARED_QUERY(?S_BLOCK, [Height]) of
        {ok, _, [Result]} ->
            {ok, block_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_block_txn_list(Height) ->
    case ?PREPARED_QUERY(?S_BLOCK_TXN_LIST, [Height]) of
        {ok, _, Result} ->
            {ok, bh_route_txns:txn_list_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_block_by_hash(BlockHash) ->
    case ?PREPARED_QUERY(?S_BLOCK_BY_HASH, [BlockHash]) of
        {ok, _, [Result]} ->
            {ok, block_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_block_by_hash_txn_list(Hash) ->
    case ?PREPARED_QUERY(?S_BLOCK_BY_HASH_TXN_LIST, [Hash]) of
        {ok, _, Result} ->
            {ok, bh_route_txns:txn_list_to_json(Result)};
        _ ->
            {error, not_found}
    end.


%%
%% json
%%

block_list_to_json(Results) ->
    lists:map(fun block_to_json/1, Results).

block_to_json({Height, Time, Hash, TxnCount}) ->
    #{
      <<"height">> => Height,
      <<"time">> => Time,
      <<"hash">> => Hash,
      <<"txns">> => TxnCount
     }.
