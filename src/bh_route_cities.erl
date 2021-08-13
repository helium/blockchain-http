-module(bh_route_cities).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_city_list/1]).

-define(S_CITY_LIST_COUNT, "city_list_count").
-define(S_CITY_LIST_COUNT_BEFORE, "city_list_count_before").
-define(S_CITY_LIST_NAME, "city_list_name").
-define(S_CITY_LIST_NAME_BEFORE, "city_list_name_before").
-define(S_CITY_BY_ID, "city_by_id").
-define(S_CITY_SEARCH, "city_search").
-define(S_CITY_SEARCH_BEFORE, "city_search_before").
-define(S_CITY_HOTSPOT_LIST, "hotspot_city_list").
-define(S_CITY_HOTSPOT_LIST_BEFORE, "hotspor_city_list_before").

-define(CITY_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    CityListLimit = "limit " ++ integer_to_list(?CITY_LIST_LIMIT),
    Loads = [
        {?S_CITY_BY_ID,
            {city_list_base, [
                {inner_scope, city_by_id_inner_scope},
                {rank, city_search_rank},
                {scope, ""},
                {order, city_search_order},
                {limit, "limit 1"}
            ]}},
        {?S_CITY_SEARCH,
            {city_list_base, [
                {inner_scope, city_search_inner_scope},
                {rank, city_search_rank},
                {scope, ""},
                {order, city_search_order},
                {limit, CityListLimit}
            ]}},
        {?S_CITY_LIST_NAME,
            {city_list_base, [
                {rank, city_list_name_rank},
                {inner_scope, ""},
                {scope, ""},
                {order, city_list_name_order},
                {limit, CityListLimit}
            ]}},
        {?S_CITY_LIST_NAME_BEFORE,
            {city_list_base, [
                {rank, city_list_name_rank},
                {inner_scope, ""},
                {scope, city_list_name_before_scope},
                {order, city_list_name_order},
                {limit, CityListLimit}
            ]}},
        {?S_CITY_LIST_COUNT,
            {city_list_base, [
                {rank, city_list_count_rank},
                {inner_scope, ""},
                {scope, ""},
                {order, city_list_count_order},
                {limit, CityListLimit}
            ]}},
        {?S_CITY_LIST_COUNT_BEFORE,
            {city_list_base, [
                {rank, city_list_count_rank},
                {inner_scope, ""},
                {scope, city_list_count_before_scope},
                {order, city_list_count_order},
                {limit, CityListLimit}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "cities.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([search, order, cursor], Req),
    Result = get_city_list(Args),
    ?MK_RESPONSE(Result, block_time);
handle('GET', [City, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([filter_modes, cursor], Req),
    CityId = ?B64_TO_BIN(City),
    try
        Result = bh_route_hotspots:get_hotspot_list([{owner, undefined}, {city, CityId} | Args]),
        ?MK_RESPONSE(Result, block_time)
    catch
        _:_ ->
            ?RESPONSE_404
    end;
handle('GET', [City], _Req) ->
    CityId = ?B64_TO_BIN(City),
    try
        Result = get_city(CityId),
        ?MK_RESPONSE(Result, block_time)
    catch
        _:_ ->
            ?RESPONSE_404
    end;
handle(_, _, _Req) ->
    ?RESPONSE_404.

order_to_rank(<<"online_count">>) ->
    <<"online_count">>;
order_to_rank(<<"offline_count">>) ->
    <<"offline_count">>;
order_to_rank(<<"hotspot_count">>) ->
    <<"hotspot_count">>;
order_to_rank(_) ->
    throw(?RESPONSE_400).

get_city(CityId) ->
    Result = ?PREPARED_QUERY(?S_CITY_BY_ID, [CityId]),
    mk_city_from_result(Result).

get_city_list([{search, undefined}, {order, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_LIST_NAME, []),
    mk_city_list_from_result(#{}, Result);
get_city_list([{search, undefined}, {order, Order}, {cursor, undefined}]) ->
    Rank = order_to_rank(Order),
    Result = ?PREPARED_QUERY(?S_CITY_LIST_COUNT, [Rank]),
    mk_city_list_from_result(#{order => Order}, Result);
get_city_list([{search, Search}, {order, _}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_SEARCH, [Search]),
    mk_city_list_from_result(#{}, Result);
get_city_list([{search, _}, {order, _}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok,
            #{
                <<"city_id">> := CityId,
                <<"rank">> := CursorRank
            } = C} ->
            case maps:get(<<"order">>, C, false) of
                false ->
                    Result = ?PREPARED_QUERY(?S_CITY_LIST_NAME_BEFORE, [CursorRank, CityId]),
                    mk_city_list_from_result(#{}, Result);
                Order ->
                    Rank = order_to_rank(Order),
                    Result = ?PREPARED_QUERY(?S_CITY_LIST_COUNT_BEFORE, [Rank, CursorRank, CityId]),
                    mk_city_list_from_result(#{order => Order}, Result)
            end;
        _ ->
            {error, badarg}
    end.

mk_city_from_result({ok, _, [Result]}) ->
    {ok, city_to_json(Result)}.

mk_city_list_from_result(CursorBase, {ok, _, Results}) ->
    {ok, city_list_to_json(Results), mk_city_list_cursor(CursorBase, Results)}.

mk_city_list_cursor(CursorBase, Results) when is_list(Results) ->
    case length(Results) < ?CITY_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {_ShortCity, _LongCity, _ShortState, _LongState, _ShortCountry, _LongCountry, CityId,
                _TotalCount, _OnlineCount, _OfflineCount, Rank} = lists:last(Results),
            CursorBase#{
                city_id => CityId,
                rank => Rank
            }
    end.

%%
%% to_jaon
%%

city_list_to_json(Results) ->
    lists:map(fun city_to_json/1, Results).

city_to_json(
    {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId, TotalCount,
        OnlineCount, OfflineCount, _Rank}
) ->
    Base = bh_route_hotspots:to_geo_json(
        {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId}
    ),
    Base#{
        hotspot_count => TotalCount,
        online_count => OnlineCount,
        offline_count => OfflineCount
    }.
