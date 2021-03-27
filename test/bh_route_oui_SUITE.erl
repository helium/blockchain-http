-module(bh_route_oui_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        get_test,
        get_invalid_test,
        list_test,
        last_test,
        stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

get_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/ouis/1"),
    #{<<"data">> := _} = Json,

    ok.

get_invalid_test(_Config) ->
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/ouis/not_int")),

    ok.

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
