-module(bh_route_ouis).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_current_oui/0]).

-define(S_OUI_CURRENT, "oui_current").

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(
        Conn,
        ?S_OUI_CURRENT,
        [
            ?SELECT_TXN_BASE,
            "from transactions t where type = 'oui_v1' ",
            "order by block desc limit 1"
        ],
        []
    ),
    #{
        ?S_OUI_CURRENT => S1
    }.

handle('GET', [], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [<<"current">>], _Req) ->
    ?MK_RESPONSE(get_current_oui(), block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"oui_v1">>}].

get_current_oui() ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_OUI_CURRENT, []),
    case Results of
        [] -> {error, not_found};
        [Result] -> {ok, bh_route_txns:txn_to_json(Result)}
    end.
