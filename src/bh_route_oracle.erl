-module(bh_route_oracle).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_price_list/1, get_current_price/0]).


-define(S_PRICE_LIST_BEFORE, "oracle_price_list_before").
-define(S_PRICE_LIST, "oracle_price_list").
-define(S_PRICE_CURRENT, "oracle_price_current").

-define(SELECT_PRICE_BASE, "select p.block, p.price from oracle_prices p ").

-define(PRICE_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_PRICE_LIST,
                            [?SELECT_PRICE_BASE,
                            "order by block DESC limit ", ?LIMIT_BLOCK_ALIGNED(?PRICE_LIST_LIMIT)
                            ], []),

    {ok, S2} = epgsql:parse(Conn, ?S_PRICE_LIST_BEFORE,
                             [?SELECT_PRICE_BASE,
                              "where p.block < $1 order by block DESC limit ", integer_to_list(?PRICE_LIST_LIMIT)
                             ], []),

    {ok, S3} = epgsql:parse(Conn, ?S_PRICE_CURRENT,
                           [?SELECT_PRICE_BASE,
                            "order by block DESC limit 1"
                           ], []),

    #{?S_PRICE_LIST => S1,
      ?S_PRICE_LIST_BEFORE => S2,
      ?S_PRICE_CURRENT => S3 }.

handle('GET', [<<"prices">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_price_list(Args), ?CACHE_TIME_BLOCK_ALIGNED(Args));
handle('GET', [<<"prices">>, <<"current">>], _Req) ->
    ?MK_RESPONSE(get_current_price(), block_time);

handle(_, _, _Req) ->
    ?RESPONSE_404.

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

get_current_price() ->
    case ?PREPARED_QUERY(?S_PRICE_CURRENT, []) of
        {ok, _, [Result]} ->
            {ok, price_to_json(Result)};
        _ ->
            {ok, price_to_json({1, 0})}
    end.


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
