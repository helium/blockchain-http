-module(bh_route_cities_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").

-include("ct_utils.hrl").

all() ->
    [
        city_list_name_test,
        city_list_count_test,
        city_search_test,
        get_test,
        city_hotspots_test,
        invalid_city_hotspots_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

city_list_name_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/cities?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

city_list_count_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities?order=hotspot_count"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/cities?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

city_search_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities?search=ma"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/cities?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

get_test(_Config) ->
    FetchId = "dG9yb250b29udGFyaW9jYW5hZGE",
    {ok, {_, _, Json}} = ?json_request(["/v1/cities/", FetchId]),
    #{
        <<"data">> := #{
            <<"city_id">> := CityId,
            <<"long_city">> := <<"Toronto">>
        }
    } = Json,
    ?assertEqual(FetchId, binary_to_list(CityId)),
    ok.

city_hotspots_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request([
        "/v1/cities/c2FuIGZyYW5jaXNjb2NhbGlmb3JuaWF1bml0ZWQgc3RhdGVz/hotspots"
    ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

invalid_city_hotspots_test(_Config) ->
    %% base64 always decodes but will cause sql to fail. We ensure here that
    %% it's always interpreted as a not_found
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/cities/not_city/hotspots")),

    ok.
