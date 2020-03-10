-module(bh_route_blocks).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_block_height/0,
         get_block_list/1,
         get_block_by_height/1,
         get_block_by_hash/1]).

-define(S_BLOCK_HEIGHT, "block_height").
-define(S_BLOCK_LIST_BEFORE, "block_list_before").
-define(S_BLOCK_LIST, "block_list").
-define(S_BLOCK_BY_HEIGHT, "block_by_height").
-define(S_BLOCK_TXN_LIST, "block_txn_list").
-define(S_BLOCK_BY_HASH, "block_by_hash").
-define(S_BLOCK_BY_HASH_TXN_LIST, "block_by_hash_txn_list").

-define(SELECT_BLOCK_BASE, "select b.height, b.time, b.block_hash, b.transaction_count from blocks b ").
-define(SELECT_BLOCK_TXN_BASE,
        "select b.height, b.time, b.block_hash, b.transaction_count, t.hash, t.type, t.fields "
        "from blocks b left join transactions t on b.height = t.block ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_HEIGHT,
                           "select max(height) from blocks", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_LIST_BEFORE,
                           ?SELECT_BLOCK_BASE "where b.height < $1 order by height DESC limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_LIST,
                           ?SELECT_BLOCK_BASE "where b.height <= (select max(height) from blocks) order by height DESC limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_BY_HEIGHT,
                           ?SELECT_BLOCK_TXN_BASE "where b.height = $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_BLOCK_BY_HASH,
                           ?SELECT_BLOCK_TXN_BASE "where b.block_hash = $1", []),

    ok.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([before, limit], Req),
    ?MK_RESPONSE(get_block_list(Args));
handle('GET', [<<"height">>], _Req) ->
    ?MK_RESPONSE(get_block_height());
handle('GET', [<<"hash">>, BlockHash], _Req) ->
    ?MK_RESPONSE(get_block_by_hash(BlockHash));
handle('GET', [BlockId], _Req) ->
    case catch binary_to_integer(BlockId) of
        {'EXIT', _} ->
            ?RESPONSE_400;
        Height ->
            ?MK_RESPONSE(get_block_by_height(Height))
    end;

handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.


get_block_list([{before, undefined}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST, [Limit]),
    {ok, block_list_to_json(Results)};
get_block_list([{before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST_BEFORE, [Before, Limit]),
    {ok, block_list_to_json(Results)}.

get_block_height() ->
    {ok, _, [{Height}]} = ?PREPARED_QUERY(?S_BLOCK_HEIGHT, []),
    {ok, #{<<"height">> => Height}}.

get_block_by_height(Height) ->
    case ?PREPARED_QUERY(?S_BLOCK_BY_HEIGHT, [Height]) of
        {ok, _, Results} when Results /= []->
            {ok, block_to_json(Results)};
        _ ->
            {error, not_found}
    end.

get_block_by_hash(BlockHash) ->
    case ?PREPARED_QUERY(?S_BLOCK_BY_HASH, [BlockHash]) of
        {ok, _, Results} when Results /= [] ->
            {ok, block_to_json(Results)};
        _ ->
            {error, not_found}
    end.

%%
%% json
%%

block_list_to_json(Results) ->
    lists:map(fun block_base_to_json/1, Results).

block_base_to_json({Height, Time, Hash, TxnCount}) ->
    #{
      <<"height">> => Height,
      <<"time">> => Time,
      <<"hash">> => Hash,
      <<"transaction_count">> => TxnCount
     }.

block_to_json(Results) ->
    %% Fold all transactions
    Txns = lists:foldl(fun block_txn_to_json/2, [], Results),
    %% The left join guarnateees there's always at least one
    %% transaction in results to pull block information from.
    {Height, Time, Hash, TxnCount, _TxnHash, _Type, _Fields} = hd(Results),
    Block = block_base_to_json({Height, Time, Hash, TxnCount}),
    Block#{
           <<"transactions">> => Txns
           }.

block_txn_to_json({_Height, _Time, _Hash, _TxnCount, null, _Type, _Fields}, _Acc) ->
    %% The left join shortcuts out to an empty transaction list if no
    %% transactions are found.
    [];
block_txn_to_json({Height, _Time, _Hash, _TxnCount, Hash, Type, Fields}, Acc) ->
    [bh_route_txns:txn_to_json({Height, Hash, Type, Fields}) | Acc].
