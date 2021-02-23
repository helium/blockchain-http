-module(bh_route_ouis).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_last_oui/0, get_oui_list/1]).

-define(S_OUI_LIST_BEFORE, "oui_list_before").
-define(S_OUI_LIST, "oui_list").
-define(S_OWNER_OUI_LIST_BEFORE, "owner_oui_list_before").
-define(S_OWNER_OUI_LIST, "owner_oui_list").
-define(S_OUI, "oui").
-define(S_LAST_OUI, "last_oui").
-define(S_ACTIVE_OUIS, "active_ouis").

-define(OUI_LIST_LIMIT, 100).

-define(S_OUI_CURRENT, "oui_current").

prepare_conn(Conn) ->
    OuiListLimit = "limit " ++ integer_to_list(?OUI_LIST_LIMIT),
    Loads = [
        {?S_OUI_LIST_BEFORE,
            {oui_list_base, [
                {scope, oui_list_before_scope},
                {order, oui_list_order},
                {limit, OuiListLimit}
            ]}},
        {?S_OUI_LIST,
            {oui_list_base, [
                {scope, ""},
                {order, oui_list_order},
                {limit, OuiListLimit}
            ]}},
        {?S_OWNER_OUI_LIST_BEFORE,
            {oui_list_base, [
                {scope, owner_oui_list_before_scope},
                {order, oui_list_order},
                {limit, OuiListLimit}
            ]}},
        {?S_OWNER_OUI_LIST,
            {oui_list_base, [
                {scope, owner_oui_list_scope},
                {order, oui_list_order},
                {limit, OuiListLimit}
            ]}},
        {?S_LAST_OUI,
            {oui_list_base, [
                {scope, "where oui = (select max(oui) from ouis)"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_OUI,
            {oui_list_base, [
                {scope, "where oui = $1"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_ACTIVE_OUIS, {oui_active, []}}
    ],

    bh_db_worker:load_from_eql(Conn, "ouis.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_oui_list([{owner, undefined} | Args]), block_time);
handle('GET', [<<"stats">>], _Req) ->
    ?MK_RESPONSE(get_stats(), block_time);
handle('GET', [<<"last">>], _Req) ->
    ?MK_RESPONSE(get_last_oui(), block_time);
handle('GET', [OuiBin], _Req) ->
    try binary_to_integer(OuiBin) of
        Oui -> ?MK_RESPONSE(get_oui(Oui), infinity)
    catch
        _:_ ->
            ?RESPONSE_400
    end;
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_oui_list([{owner, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OUI_LIST, []),
    mk_oui_list_from_result(Result);
get_oui_list([{owner, Owner}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OWNER_OUI_LIST, [Owner]),
    mk_oui_list_from_result(Result);
get_oui_list([{owner, Owner}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            case Owner of
                undefined ->
                    Result =
                        ?PREPARED_QUERY(?S_OUI_LIST_BEFORE, [
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_oui_list_from_result(Result);
                Owner ->
                    Result =
                        ?PREPARED_QUERY(?S_OWNER_OUI_LIST_BEFORE, [
                            Owner,
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_oui_list_from_result(Result)
            end;
        _ ->
            {error, badarg}
    end.

get_oui(Oui) ->
    case ?PREPARED_QUERY(?S_OUI, [Oui]) of
        {ok, _, [Result]} -> {ok, oui_to_json(Result)};
        _ -> {error, not_found}
    end.

get_last_oui() ->
    case ?PREPARED_QUERY(?S_LAST_OUI, []) of
        {ok, _, [Result]} -> {ok, oui_to_json(Result)};
        _ -> {error, not_found}
    end.

get_stats() ->
    CountStats = ?PREPARED_QUERY(?S_ACTIVE_OUIS, []),
    mk_stats_from_results(CountStats).

mk_oui_list_from_result({ok, _, Results}) ->
    {ok, oui_list_to_json(Results), mk_cursor(Results)}.

mk_stats_from_results({ok, _, [{Count}]}) ->
    {ok, #{count => Count}}.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?OUI_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {Height, Oui, _Owner, _Nonce, _Addresses, _Subnets, FirstBlock} = lists:last(Results),
            #{
                before_oui => Oui,
                before_block => FirstBlock,
                height => Height
            }
    end.

%%
%% json
%%

oui_list_to_json(Results) ->
    lists:map(fun oui_to_json/1, Results).

oui_to_json(
    {Height, Oui, Owner, Nonce, Addresses, Subnets, _FirstBlock}
) ->
    MkSubnet = fun
        ([Base, Mask], Acc) ->
            [#{base => Base, mask => Mask} | Acc];
        (_, Acc) ->
            Acc
    end,

    #{
        block => Height,
        oui => Oui,
        owner => Owner,
        nonce => Nonce,
        addresses => Addresses,
        subnets => lists:foldl(MkSubnet, [], Subnets)
    }.
