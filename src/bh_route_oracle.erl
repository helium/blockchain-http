-module(bh_route_oracle).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_price_list/1, get_price_at_block/1]).


-define(S_PRICE_LIST_BEFORE, "oracle_price_list_before").
-define(S_PRICE_LIST, "oracle_price_list").
-define(S_PRICE_AT_BLOCK, "oracle_price_at_block").
-define(S_PRICE_PREDICTIONS, "oracle_price_predictions").

-define(SELECT_PRICE_BASE, "select p.block, p.price from oracle_prices p ").

-define(PRICE_LIST_LIMIT, 100).
-define(PRICE_LIST_BLOCK_ALIGN, 100).

prepare_conn(Conn) ->
    PriceListLimit = integer_to_list(?PRICE_LIST_LIMIT),
    {ok, S1} = epgsql:parse(Conn, ?S_PRICE_LIST,
                            [?SELECT_PRICE_BASE,
                             "order by block DESC limit ", PriceListLimit
                            ], []),

    {ok, S2} = epgsql:parse(Conn, ?S_PRICE_LIST_BEFORE,
                             [?SELECT_PRICE_BASE,
                              "where p.block < $1 order by block DESC limit ", PriceListLimit
                             ], []),

    {ok, S3} = epgsql:parse(Conn, ?S_PRICE_AT_BLOCK,
                           [?SELECT_PRICE_BASE,
                            "where block <= coalesce($1, (select max(height) from blocks)) "
                            "order by block DESC limit 1"
                           ], []),

    {ok, S4} = epgsql:parse(Conn, ?S_PRICE_PREDICTIONS,
                           ["select time, price from oracle_price_predictions ",
                            "order by time DESC"
                           ], []),

    #{
      ?S_PRICE_LIST => S1,
      ?S_PRICE_LIST_BEFORE => S2,
      ?S_PRICE_AT_BLOCK => S3,
      ?S_PRICE_PREDICTIONS => S4
     }.

handle('GET', [<<"prices">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_price_list(Args), block_time);
handle('GET', [<<"prices">>, <<"current">>], _Req) ->
    ?MK_RESPONSE(get_price_at_block(undefined), block_time);
handle('GET', [<<"prices">>, Block], _Req) ->
    try binary_to_integer(Block) of
        Height -> ?MK_RESPONSE(get_price_at_block(Height), infinity)
    catch _:_ ->
        ?RESPONSE_400
    end;
handle('GET', [<<"predictions">>], _Req) ->
    ?MK_RESPONSE(get_price_predictions(), block_time);
handle('GET', [<<"activity">>], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Address, <<"activity">>], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    Result = bh_route_txns:get_actor_txn_list({actor, Address}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);

handle(_, _, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"price_oracle_v1">>}].

get_price_list([{cursor, undefined}])  ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_PRICE_LIST, []),
    {ok, price_list_to_json(Results), mk_price_list_cursor(undefined, Results)};
get_price_list([{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before">> := Before}} ->
            {ok, _, Results} = ?PREPARED_QUERY(?S_PRICE_LIST_BEFORE, [Before - (Before rem ?PRICE_LIST_LIMIT)]),
            {ok, price_list_to_json(Results), mk_price_list_cursor(Cursor, Results)};
        _ ->
            {error, badarg}
    end.

mk_price_list_cursor(PrevCursor, Results) when is_list(Results) ->
    case length(Results) of
        0 -> undefined;
        N when (N < ?PRICE_LIST_LIMIT) and not (PrevCursor == undefined) ->
            %% We have a cursor and we didn't get the full length. We
            %% must have reached the end of available data.
            undefined;
        _ ->
            {Block, _Price} = lists:last(Results),
            #{ before => Block }
    end.

get_price_at_block(Height) ->
    case ?PREPARED_QUERY(?S_PRICE_AT_BLOCK, [Height]) of
        {ok, _, [Result]} ->
            {ok, price_to_json(Result)};
        _ ->
            {ok, price_to_json({1, 0})}
    end.

get_price_predictions() ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_PRICE_PREDICTIONS, []),
    {ok, price_predictions_to_json(Results)}.


%%
%% json
%%

price_list_to_json(Results) ->
    lists:map(fun price_to_json/1, Results).

price_to_json({Block, Price}) ->
    #{
      block => Block,
      price => Price
     }.

price_predictions_to_json(Results) ->
    lists:map(fun price_prediction_to_json/1, Results).

price_prediction_to_json({Time, Price}) ->
    #{
      time => Time,
      price => Price
     }.
