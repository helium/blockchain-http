-module(bh_route_hotspots_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").

-include("ct_utils.hrl").

all() -> [
          list_test,
          get_test,
          not_found_test,
          activity_result_test,
          activity_low_block_test,
          activity_filter_no_result_test,
          elections_test,
          challenges_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).


list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots"]),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/hotspots?cursor=", Cursor]),
    #{ <<"data">> := NextData } = NextJson,
    ?assert(length(NextData) >= 0),
    ok.

get_test(_Config) ->
    FetchAddress = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", FetchAddress]),
    #{ <<"data">> :=
           #{
             <<"address">> := Address
            }
     } = Json,
    ?assertEqual(FetchAddress, binary_to_list(Address)),
    ok.

not_found_test(_Config) ->
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/hotspots/no_address")),
    ok.

activity_result_test(_Config) ->
    %% Test activity for a hotspot. This may or may not have data
    %% returned. Expect a maybe empty array with a start and end block
    %% and a cursor to a next block range
    {ok, {_, _, Json}} = ?json_request("/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity"),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor
     } = Json,
    {ok,
     #{ <<"block">> := _
      }
    } = ?CURSOR_DECODE(Cursor),
    ?assert(length(Data) =< ?TXN_LIST_LIMIT).

activity_low_block_test(_Config) ->
    GetCursor = #{ block => 50 },
    {ok, {_, _, Json}} = ?json_request(
                            ["/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity",
                             "?cursor=", binary_to_list(?CURSOR_ENCODE(GetCursor))
                            ]),
    #{ <<"data">> := Data
     } = Json,
    %% This hotspot has no activity in the low blocks
    ?assertEqual(0, length(Data)),
    ?assertEqual(undefined, maps:get(<<"cursor">>, Json, undefined)).

activity_filter_no_result_test(_Config) ->
    %% Filter for no rewards, which the given hotspot should not have
    GetCursor = #{ block => 50,
                   types => <<"rewards_v1">>
                 },
    {ok, {_, _, Json}} = ?json_request(
                            ["/v1/hotspots/112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK/activity",
                             "?cursor=", binary_to_list(?CURSOR_ENCODE(GetCursor))
                            ]),
    #{ <<"data">> := Data } = Json,
    ?assertEqual(0, length(Data)),
    ok.


elections_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", Hotspot, "/elections"]),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/hotspots/", Hotspot, "/elections?cursor=", Cursor]),
    #{ <<"data">> := NextData } = NextJson,
    ?assert(length(NextData) >= 0),

    ok.

challenges_test(_Config) ->
    Hotspot = "112DCTVEbFi8azQ2KmhSDW2UqRM2ijmiMWKJptnhhPEk3uXvwLyK",
    {ok, {_, _, Json}} = ?json_request(["/v1/hotspots/", Hotspot, "/challenges"]),
    #{ <<"data">> := Data,
       <<"cursor">> := Cursor } = Json,
    ?assert(length(Data) >= 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/hotspots/", Hotspot, "/challenges?cursor=", Cursor]),
    #{ <<"data">> := NextData } = NextJson,
    ?assert(length(NextData) >= 0),

    ok.

