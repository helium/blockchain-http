-module(bh_route_dc_burns_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        list_test,
        stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/dc_burns"),
    #{<<"data">> := Data, <<"cursor">> := Cursor} = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(
        [
            "/v1/dc_burns",
            "?cursor=",
            Cursor
        ]
    ),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),

    ok.

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/dc_burns/stats"),
    #{<<"data">> := #{<<"last_day">> := Value}} = Json,
    ?assert(Value >= 0),
    ok.
