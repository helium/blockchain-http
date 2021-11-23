-module(bh_route_locations).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_location/1]).

-define(S_LOCATION, "location_at_index").

prepare_conn(_Conn) ->
    Loads = [
        {?S_LOCATION,
            {location_list_base,
                [
                    {scope, "where location = $1"}
                ],
                [text]}}
    ],
    bh_db_worker:load_from_eql("locations.sql", Loads).

handle('GET', [Location], _Req) ->
    ?MK_RESPONSE(get_location(Location), infinity);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_location(Location) ->
    case ?PREPARED_QUERY(?S_LOCATION, [Location]) of
        {ok, _, [Result]} ->
            {ok, location_to_json(Result)};
        _ ->
            {error, not_found}
    end.

%%
%% json
%%

location_to_json(
    {ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry,
        CityId, Location}
) ->
    MaybeB64 = fun
        (null) -> null;
        (Bin) -> ?BIN_TO_B64(Bin)
    end,
    #{
        short_street => ShortStreet,
        long_street => LongStreet,
        short_city => ShortCity,
        long_city => LongCity,
        short_state => ShortState,
        long_state => LongState,
        short_country => ShortCountry,
        long_country => LongCountry,
        city_id => MaybeB64(CityId),
        location => Location
    }.
