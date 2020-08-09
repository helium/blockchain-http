-module(bh_route_cities_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").

-include("ct_utils.hrl").

all() -> [
          city_list_test,
          city_search_test,
          city_hotspots_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).



city_list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities"]),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/cities?cursor=", Cursor]),
    #{ <<"data">> := NextData } = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

city_search_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities?search=ma"]),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/cities?cursor=", Cursor]),
    #{ <<"data">> := NextData } = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

city_hotspots_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/cities/c2FuIGZyYW5jaXNjb2NhbGlmb3JuaWF1bml0ZWQgc3RhdGVz/hotspots"]),
    #{ <<"data">> := Data } = Json,
    ?assert(length(Data) >= 0),

    ok.
