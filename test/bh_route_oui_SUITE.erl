-module(bh_route_oui_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        list_test,
        last_test,
        stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

last_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/ouis/last"),
    #{<<"data">> := _} = Json,

    ok.

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/ouis"),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/ouis/stats"]),
    #{<<"data">> := Data} = Json,
    ?assert(maps:size(Data) > 0),

    ok.
