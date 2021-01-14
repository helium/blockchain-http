-module(bh_route_locations_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        get_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

get_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/locations/8c28347213117ff"),
    #{<<"data">> := _} = Json,

    ok.
