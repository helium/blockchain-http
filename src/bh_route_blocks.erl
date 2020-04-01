-module(bh_route_blocks).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_block_height/0,
         get_block_list/1,
         get_block_by_height/2,
         get_block_by_hash/2]).

-define(S_BLOCK_HEIGHT, "block_height").

-define(S_BLOCK_LIST, "block_list").
-define(S_BLOCK_LIST_BEFORE, "block_list_before").

-define(S_BLOCK_BY_HEIGHT, "block_by_height").
-define(S_BLOCK_BY_HEIGHT_BEFORE, "block_by_height_before").

-define(S_BLOCK_BY_HASH, "block_by_hash").
-define(S_BLOCK_BY_HASH_BEFORE, "block_by_hash_before").

-define(SELECT_BLOCK_BASE, "select b.height, b.time, b.block_hash, b.prev_hash, b.transaction_count from blocks b ").
-define(SELECT_BLOCK_TXN_BASE(C),
        "select * from "
        "(select b.height, b.time, b.block_hash, b.prev_hash, b.transaction_count, t.hash, t.type, t.fields "
        "from blocks b left join transactions t on b.height = t.block "
        "where b." C " = $1 order by t.hash) r "
       ).

-define(BLOCK_LIST_LIMIT, 100).
-define(BLOCK_TXN_LIST_LIMIT, 50).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_BLOCK_HEIGHT,
                           "select max(height) from blocks", []),

    {ok, S2} = epgsql:parse(Conn, ?S_BLOCK_LIST,
                           [?SELECT_BLOCK_BASE,
                            "order by height DESC limit (select max(height) % ", integer_to_list(?BLOCK_LIST_LIMIT), " from blocks)"],
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_BLOCK_LIST_BEFORE,
                           [?SELECT_BLOCK_BASE,
                            "where b.height < $1 order by height DESC limit ", integer_to_list(?BLOCK_LIST_LIMIT)],
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_BLOCK_BY_HEIGHT,
                            [?SELECT_BLOCK_TXN_BASE("height") "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)],
                            []),

    {ok, S5} = epgsql:parse(Conn, ?S_BLOCK_BY_HEIGHT_BEFORE,
                           [?SELECT_BLOCK_TXN_BASE("height"),
                            "where r.hash > $2 limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)],
                            []),

    {ok, S6} = epgsql:parse(Conn, ?S_BLOCK_BY_HASH,
                           [?SELECT_BLOCK_TXN_BASE("block_hash"),
                            "limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)],
                            []),

    {ok, S7} = epgsql:parse(Conn, ?S_BLOCK_BY_HASH_BEFORE,
                           [?SELECT_BLOCK_TXN_BASE("block_hash"),
                            "where r.hash > $2 limit ", integer_to_list(?BLOCK_TXN_LIST_LIMIT)],
                            []),

    #{?S_BLOCK_HEIGHT => S1,
      ?S_BLOCK_LIST => S2,
      ?S_BLOCK_LIST_BEFORE => S3,
      ?S_BLOCK_BY_HEIGHT => S4,
      ?S_BLOCK_BY_HEIGHT_BEFORE => S5,
      ?S_BLOCK_BY_HASH => S6,
      ?S_BLOCK_BY_HASH_BEFORE => S7}.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    CacheTime = case Args of
                    [{cursor, undefined}] -> block_time;
                    _ -> infinity
                end,
    ?MK_RESPONSE(get_block_list(Args), CacheTime);
handle('GET', [<<"height">>], _Req) ->
    ?MK_RESPONSE(get_block_height(), block_time);
handle('GET', [<<"hash">>, BlockHash], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_block_by_hash(BlockHash, Args), infinity);
handle('GET', [BlockId], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    try binary_to_integer(BlockId) of
        Height -> ?MK_RESPONSE(get_block_by_height(Height, Args), infinity)
    catch _:_ ->
        ?RESPONSE_400
    end;

handle(_Method, _Path, _Req) ->
    ?RESPONSE_404.


get_block_list([{cursor, undefined}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST, []),
    {ok, block_list_to_json(Results), mk_cursor(Results)};
get_block_list([{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before">> := Before}} ->
            {ok, _, Results} = ?PREPARED_QUERY(?S_BLOCK_LIST_BEFORE, [Before - (Before rem ?BLOCK_LIST_LIMIT)]),
            {ok, block_list_to_json(Results), mk_cursor(Results)};
        _ ->
            {error, badarg}
    end.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) of
        0 -> undefined;
        _ -> case lists:last(Results) of
                 {Height, _Time, _Hash, _PrevHash, _TxnCount} when Height == 1 -> undefined;
                 {Height, _Time, _Hash, _PrevHash, _TxnCount}  -> #{ before => Height}
             end
    end.

get_block_height() ->
    {ok, _, [{Height}]} = ?PREPARED_QUERY(?S_BLOCK_HEIGHT, []),
    {ok, #{<<"height">> => Height}}.

get_block_by_height(BlockHeight, [{cursor, undefined}]) ->
    mk_block_txn_list_from_result(?PREPARED_QUERY(?S_BLOCK_BY_HEIGHT, [BlockHeight]));
get_block_by_height(BlockHeight, [{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"hash">> := TxnHash}} ->
            Results = ?PREPARED_QUERY(?S_BLOCK_BY_HEIGHT_BEFORE, [BlockHeight, TxnHash]),
            mk_block_txn_list_from_result(Results);
        _ ->
            {error, badarg}
    end.


get_block_by_hash(BlockHash, [{cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_BLOCK_BY_HASH, [BlockHash]),
    mk_block_txn_list_from_result(Result);
get_block_by_hash(BlockHash, [{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"hash">> := TxnHash}} ->
            Result = ?PREPARED_QUERY(?S_BLOCK_BY_HASH_BEFORE, [BlockHash, TxnHash]),
            mk_block_txn_list_from_result(Result);
        _ ->
            {error, badarg}
    end.

mk_block_txn_list_from_result({ok, _, Results}) when Results /= [] ->
    {ok, block_to_json(Results), mk_block_txn_list_cursor(Results)};
mk_block_txn_list_from_result(_) ->
    {error, not_found}.

mk_block_txn_list_cursor(Results) ->
    case Results of
        [{_Height, _Time, _Hash, _TxnCount, null, _Type, _Fields}] ->
            undefined;
        _ when length(Results) < ?BLOCK_TXN_LIST_LIMIT ->
            undefined;
        _ ->
            {_Height, _Time, _Hash, _TxnCount, TxnHash, _Type, _Fields} = lists:last(Results),
            #{ hash => TxnHash }
    end.

%%
%% json
%%

block_list_to_json(Results) ->
    lists:map(fun block_base_to_json/1, Results).

block_base_to_json({Height, Time, Hash, PrevHash, TxnCount}) ->
    #{
      height => Height,
      time => Time,
      hash => Hash,
      prev_hash => PrevHash,
      transaction_count => TxnCount
     }.

block_to_json(Results) ->
    %% Fold all transactions
    Txns = lists:foldl(fun block_txn_to_json/2, [], Results),
    %% The left join guarnateees there's always at least one
    %% transaction in results to pull block information from.
    {Height, Time, Hash, PrevHash, TxnCount, _TxnHash, _Type, _Fields} = hd(Results),
    Block = block_base_to_json({Height, Time, Hash, PrevHash, TxnCount}),
    Block#{
           transactions => Txns
           }.

block_txn_to_json({_Height, _Time, _Hash, _PrevHash, _TxnCount, null, _Type, _Fields}, _Acc) ->
    %% The left join shortcuts out to an empty transaction list if no
    %% transactions are found.
    [];
block_txn_to_json({Height, Time, _Hash, _PrevHash, _TxnCount, TxnHash, Type, Fields}, Acc) ->
    [bh_route_txns:txn_to_json({Height, Time, TxnHash, Type, Fields}) | Acc].
