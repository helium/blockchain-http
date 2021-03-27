-module(bh_route_cities).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_city_list/1]).

-define(S_CITY_LIST, "city_list").
-define(S_CITY_LIST_BEFORE, "city_list_before").
-define(S_CITY_SEARCH, "city_search").
-define(S_CITY_SEARCH_BEFORE, "city_search_before").
-define(S_CITY_HOTSPOT_LIST, "hotspot_city_list").
-define(S_CITY_HOTSPOT_LIST_BEFORE, "hotspor_city_list_before").

-define(SELECT_CITY_LIST_BASE, [
    "select ",
    "  short_city, long_city, ",
    "  short_state, long_state, ",
    "  short_country, long_country, ",
    "  city_id, rank, hotspot_count ",
    "from ",
    "  (select",
    "     last(l.short_city) as short_city, l.long_city, ",
    "     last(l.short_state) as short_state, l.long_state, ",
    "     last(l.short_country) as short_country, l.long_country, ",
    "     last(l.city_id) as city_id, ",
    "     count(*) as hotspot_count, ",
    "     1 as rank ",
    "   from locations l inner join gateway_inventory g on g.location = l.location "
    "   group by (l.long_country, l.long_state, l.long_city) ",
    "   order by city_id ",
    "   ) c "
]).

-define(SELECT_CITY_SEARCH_BASE, [
    "select ",
    "  short_city, long_city, ",
    "  short_state, long_state, ",
    "  short_country, long_country, ",
    "  city_id, rank, hotspot_count ",
    "from ",
    "  (select",
    "     last(l.short_city) as short_city, l.long_city, ",
    "     last(l.short_state) as short_state, l.long_state, ",
    "     last(l.short_country) as short_country, l.long_country, ",
    "     last(l.city_id) as city_id, ",
    "     count(*) as hotspot_count, ",
    "     word_similarity(l.long_city, $1) as rank ",
    "   from locations l inner join gateway_inventory g on g.location = l.location "
    "   where l.search_city %> lower($1) ",
    "   group by (l.long_country, l.long_state, l.long_city) ",
    "   order by rank desc, city_id ",
    "   ) c "
]).

-define(CITY_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(
        Conn,
        ?S_CITY_SEARCH,
        [
            ?SELECT_CITY_SEARCH_BASE,
            "limit ",
            integer_to_list(?CITY_LIST_LIMIT)
        ],
        []
    ),

    {ok, S2} = epgsql:parse(
        Conn,
        ?S_CITY_SEARCH_BEFORE,
        [
            ?SELECT_CITY_SEARCH_BASE,
            "where rank <= $2 and city_id > $3 ",
            "limit ",
            integer_to_list(?CITY_LIST_LIMIT)
        ],
        []
    ),

    {ok, S3} = epgsql:parse(
        Conn,
        ?S_CITY_LIST,
        [
            ?SELECT_CITY_LIST_BASE,
            "limit ",
            integer_to_list(?CITY_LIST_LIMIT)
        ],
        []
    ),

    {ok, S4} = epgsql:parse(
        Conn,
        ?S_CITY_LIST_BEFORE,
        [
            ?SELECT_CITY_LIST_BASE,
            "where rank <= $1 and city_id > $2 ",
            "limit ",
            integer_to_list(?CITY_LIST_LIMIT)
        ],
        []
    ),

    #{
        ?S_CITY_SEARCH => S1,
        ?S_CITY_SEARCH_BEFORE => S2,
        ?S_CITY_LIST => S3,
        ?S_CITY_LIST_BEFORE => S4
    }.

handle('GET', [], Req) ->
    Args = ?GET_ARGS([search, cursor], Req),
    Result = get_city_list(Args),
    ?MK_RESPONSE(Result, block_time);
handle('GET', [City, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    CityId = ?B64_TO_BIN(City),
    try
        Result = bh_route_hotspots:get_hotspot_list([{owner, undefined}, {city, CityId} | Args]),
        ?MK_RESPONSE(Result, block_time)
    catch
        _:_ ->
            ?RESPONSE_404
    end;
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_city_list([{search, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_LIST, []),
    mk_city_list_from_result(undefined, Result);
get_city_list([{search, Search}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_SEARCH, [Search]),
    mk_city_list_from_result(Search, Result);
get_city_list([{search, _Search}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok,
            #{
                <<"city_id">> := CityId,
                <<"rank">> := Rank
            } = C} ->
            Search = maps:get(<<"search">>, C, undefined),
            Result =
                case Search of
                    undefined ->
                        ?PREPARED_QUERY(?S_CITY_LIST_BEFORE, [Rank, CityId]);
                    Search ->
                        ?PREPARED_QUERY(?S_CITY_SEARCH_BEFORE, [Search, Rank, CityId])
                end,
            mk_city_list_from_result(Search, Result);
        _ ->
            {error, badarg}
    end.

mk_city_list_from_result(Search, {ok, _, Results}) ->
    {ok, city_list_to_json(Results), mk_city_list_cursor(Search, Results)}.

mk_city_list_cursor(Search, Results) when is_list(Results) ->
    case length(Results) < ?CITY_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {_ShortCity, _LongCity, _ShortState, _LongState, _ShortCountry, _LongCountry, CityId,
                Rank, _Count} = lists:last(Results),
            Base = #{
                city_id => CityId,
                rank => Rank
            },
            case Search of
                undefined -> Base;
                _ -> Base#{search => Search}
            end
    end.

%%
%% to_jaon
%%

city_list_to_json(Results) ->
    lists:map(fun city_to_json/1, Results).

city_to_json(
    {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId, _Rank, Count}
) ->
    Base = bh_route_hotspots:to_geo_json(
        {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId}
    ),
    Base#{
        hotspot_count => Count
    }.
