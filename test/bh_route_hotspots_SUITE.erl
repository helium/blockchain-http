-module(bh_route_hotspots_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").
-include("ct_utils.hrl").

all() ->
    [
        list_test,
        get_test,
        not_found_test,
        activity_count_test,
        activity_result_test,
        activity_low_block_test,
        activity_filter_no_result_test,
        elections_test,
        elected_test,
        elected_block_test,
        elected_invalid_block_test,
        elected_hash_test,
        challenges_test,
        rewards_test,
        rewards_all_sum_test,
        rewards_sum_test,
        rewards_buckets_test,
        witnesses_test,
        witnessed_test,
        witnesses_buckets_test,
        challenges_buckets_test,
        name_test,
        name_search_test,
        location_distance_search_test,
        location_box_search_test,
        location_hex_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) > 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/hotspots?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) > 0),
    ok.

get_test(_Config) ->
    FetchAddress = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", FetchAddress]),
    #{
        <<"data">> := #{
            <<"address">> := Address
        }
    } = Json,
    ?assertEqual(FetchAddress, binary_to_list(Address)),
    ok.

not_found_test(_Config) ->
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/hotspots/no_address")),
    ok.

activity_count_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/hotspots/",
        Hotspot,
        "/activity/count?filter_types=add_gateway_v1,assert_location_v1"
    ]),
    #{
        <<"data">> := Data
    } = Json,
    ?assertEqual(2, maps:size(Data)),
    ?assertEqual(maps:get(<<"add_gateway_v1">>, Data), 1),
    ?assert(maps:get(<<"assert_location_v1">>, Data) >= 0),
    ok.

activity_result_test(_Config) ->
    %% Test activity for a hotspot. This may or may not have data
    %% returned. Expect a maybe empty array with a start and end block
    %% and a cursor to a next block range
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity",
            "?limit=5"
        ]
    ),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    {ok, #{<<"block">> := _}} = ?CURSOR_DECODE(Cursor),
    ?assert(length(Data) =< 5).

activity_low_block_test(_Config) ->
    GetCursor = #{
        block => 50,
        max_block => 50,
        min_block => 1
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    %% This hotspot has no activity in the low blocks
    ?assertEqual(0, length(Data)),
    ?assertEqual(undefined, maps:get(<<"cursor">>, Json, undefined)).

activity_filter_no_result_test(_Config) ->
    %% Filter for no rewards, which the given hotspot should not have
    GetCursor = #{
        block => 50,
        max_block => 50,
        min_block => 1,
        types => <<"rewards_v1">>
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    ?assertEqual(0, length(Data)),
    ok.

elections_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", Hotspot, "/elections"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} =
        ?json_request(["/v1/hotspots/", Hotspot, "/elections?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),

    ok.

elected_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/elected"]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) >= 0),

    ok.

elected_block_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/elected/93"]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) > 0),

    ok.

elected_invalid_block_test(_Config) ->
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/hotspots/elected/not_int")),

    ok.

elected_hash_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/elected/hash/5cBTF9x8DN1DhUNVmQExXHWAXID6BZIxJG-LLx4KzSs"
        ]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) > 0),

    ok.

challenges_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", Hotspot, "/challenges"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} =
        ?json_request(["/v1/hotspots/", Hotspot, "/challenges?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) >= 0),

    ok.

rewards_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/rewards?max_time=2020-09-27&min_time=2020-08-27"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    case maps:get(<<"cursor">>, Json, undefined) of
        undefined ->
            ok;
        Cursor ->
            {ok, {_, _, CursorJson}} =
                ?json_request(["/v1/hotspots/", Hotspot, "/rewards?cursor=", Cursor]),
            #{<<"data">> := CursorData} = CursorJson,
            ?assert(length(CursorData) >= 0)
    end,
    ok.

rewards_all_sum_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            "/rewards/sum?max_time=2020-08-27&min_time=2020-07-27"
        ]),
    #{<<"data">> := #{<<"sum">> := Sum}} = Json,
    ?assert(Sum >= 0),

    ok.

rewards_sum_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/rewards/sum?max_time=2020-08-27&min_time=2020-07-27"
        ]),
    #{<<"data">> := #{<<"sum">> := Sum}} = Json,
    ?assert(Sum >= 0),

    ok.

rewards_buckets_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/rewards/sum?max_time=2020-09-27&min_time=2020-08-27&bucket=day"
        ]),
    #{<<"data">> := Data} = Json,
    ?assertEqual(31, length(Data)),

    ok.

witnesses_test(_Config) ->
    Hotspot = "112hYxknRPeCP9PLtkAy3f86fWpXaRzRffjPj5HcrS7qePttY3Ek",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/witnesses"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

witnessed_test(_Config) ->
    Hotspot = "112hYxknRPeCP9PLtkAy3f86fWpXaRzRffjPj5HcrS7qePttY3Ek",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/witnessed"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

witnesses_buckets_test(_Config) ->
    Hotspot = "112hYxknRPeCP9PLtkAy3f86fWpXaRzRffjPj5HcrS7qePttY3Ek",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/witnesses/sum?max_time=2020-09-27&min_time=2020-08-27&bucket=day"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

challenges_buckets_test(_Config) ->
    Hotspot = "11jck39wYcLCWn4g8gZNgNfLZsyJUemLm2k7wk7uKbgfNG1CpNc",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/hotspots/",
            Hotspot,
            "/challenges/sum?min_time=2021-11-15T16%3A48%3A00Z&max_time=2021-11-29T16%3A48%3A00Z&bucket=week"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

name_test(_Config) ->
    FetchName = "clever-tan-panther",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/name/", FetchName]),
    #{
        <<"data">> := Results
    } = Json,
    ?assert(length(Results) >= 1),
    ok.

name_search_test(_Config) ->
    Search = "clever",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/name?search=", Search]),
    #{
        <<"data">> := Results
    } = Json,
    ?assert(length(Results) >= 1),
    ok.

location_distance_search_test(_Config) ->
    Lat = "38.12129445739087",
    Lon = "-122.52885074963571",
    Distance = "100000",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/hotspots/location/distance?lat=",
        Lat,
        "&lon=",
        Lon,
        "&distance=",
        Distance
    ]),
    #{
        <<"data">> := Results,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Results) >= 1),
    {ok, {_, _, CursorJson}} = ?json_request([
        "/v1/hotspots/location/distance?cursor=",
        Cursor
    ]),
    #{
        <<"data">> := CursorResults
    } = CursorJson,
    ?assert(length(CursorResults) >= 1),
    ok.

location_box_search_test(_Config) ->
    SWLat = "37.7299412",
    SWLon = "-122.5090477",
    NELat = "37.8047205",
    NELon = "-122.3777419",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/hotspots/location/box?swlat=",
        SWLat,
        "&swlon=",
        SWLon,
        "&nelat=",
        NELat,
        "&nelon=",
        NELon
    ]),
    #{
        <<"data">> := Results,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Results) >= 1),
    {ok, {_, _, CursorJson}} = ?json_request([
        "/v1/hotspots/location/box?cursor=",
        Cursor
    ]),
    #{
        <<"data">> := CursorResults
    } = CursorJson,
    ?assert(length(CursorResults) >= 1),
    ok.

location_hex_test(_Config) ->
    %% San Francisco should have a few in res 8
    FetchLocation = "88283082a3fffff",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/hex/", FetchLocation]),
    #{
        <<"data">> := Results
    } = Json,
    ?assert(length(Results) >= 1),
    ok.
