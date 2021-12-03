-module(bh_route_validators_SUITE).

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
        roles_count_test,
        roles_result_test,
        roles_low_block_test,
        roles_filter_no_result_test,
        elected_test,
        elected_block_test,
        elected_invalid_block_test,
        elected_hash_test,
        rewards_test,
        rewards_all_sum_test,
        rewards_sum_test,
        rewards_buckets_test,
        name_test,
        name_search_test,
        stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/validators"]),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    ?assert(length(Data) > 0),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/validators?cursor=", Cursor]),
    #{<<"data">> := NextData} = NextJson,
    ?assert(length(NextData) > 0),
    ok.

get_test(_Config) ->
    FetchAddress = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/", FetchAddress]),
    #{
        <<"data">> := #{
            <<"address">> := Address
        }
    } = Json,
    ?assertEqual(FetchAddress, binary_to_list(Address)),
    ok.

not_found_test(_Config) ->
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/validators/no_address")),
    ok.

activity_count_test(_Config) ->
    Validator = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/validators/",
        Validator,
        "/activity/count?filter_types=validator_heartbeat_v1"
    ]),
    #{
        <<"data">> := Data
    } = Json,
    ?assertEqual(1, maps:size(Data)),
    ?assert(maps:get(<<"validator_heartbeat_v1">>, Data) > 0),
    ok.

activity_result_test(_Config) ->
    %% Test activity for a hotspot. This may or may not have data
    %% returned. Expect a maybe empty array with a start and end block
    %% and a cursor to a next block range
    {ok, {_, _, Json}} = ?json_request(
        "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/activity"
    ),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    {ok, #{<<"block">> := _}} = ?CURSOR_DECODE(Cursor),
    ?assert(length(Data) =< ?TXN_LIST_LIMIT).

activity_low_block_test(_Config) ->
    GetCursor = #{
        block => 50,
        max_block => 50,
        min_block => 1
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/activity",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    %% This validator has no activity in the low blocks
    ?assertEqual(0, length(Data)),
    ?assertEqual(undefined, maps:get(<<"cursor">>, Json, undefined)).

activity_filter_no_result_test(_Config) ->
    %% Filter for no rewards, which the given hotspot should not have
    GetCursor = #{
        block => 50,
        min_block => 1,
        max_block => 50,
        types => <<"rewards_v1">>
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/activity",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    ?assertEqual(0, length(Data)),
    ok.

roles_count_test(_Config) ->
    Validator = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/validators/",
        Validator,
        "/roles/count?filter_types=validator_heartbeat_v1"
    ]),
    #{
        <<"data">> := Data
    } = Json,
    ?assertEqual(1, maps:size(Data)),
    ?assert(maps:get(<<"validator_heartbeat_v1">>, Data) > 0),
    ok.

roles_result_test(_Config) ->
    %% Test activity for a validator. This may or may not have data
    %% returned. Expect a maybe empty array with a start and end block
    %% and a cursor to a next block range
    {ok, {_, _, Json}} = ?json_request(
        "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/roles"
    ),
    #{
        <<"data">> := Data,
        <<"cursor">> := Cursor
    } = Json,
    {ok, #{<<"block">> := _}} = ?CURSOR_DECODE(Cursor),
    ?assert(length(Data) =< ?TXN_LIST_LIMIT).

roles_low_block_test(_Config) ->
    GetCursor = #{
        block => 50,
        max_block => 50,
        min_block => 1
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/roles",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    %% This validator has no activity in the low blocks
    ?assertEqual(0, length(Data)),
    ?assertEqual(undefined, maps:get(<<"cursor">>, Json, undefined)).

roles_filter_no_result_test(_Config) ->
    %% Filter for no rewards, which the given hotspot should not have
    GetCursor = #{
        block => 50,
        min_block => 1,
        max_block => 50,
        types => <<"rewards_v1">>
    },
    {ok, {_, _, Json}} = ?json_request(
        [
            "/v1/validators/11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW/roles",
            "?cursor=",
            binary_to_list(?CURSOR_ENCODE(GetCursor))
        ]
    ),
    #{<<"data">> := Data} = Json,
    ?assertEqual(0, length(Data)),
    ok.

elected_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/elected"]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) >= 0),

    ok.

elected_block_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/elected/910550"]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) > 0),

    ok.

elected_invalid_block_test(_Config) ->
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/validators/elected/not_int")),

    ok.

elected_hash_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/validators/elected/hash/icmV8BofCxxVi1aS33zKsodrdngjgilBIsTGRYStd_s"
        ]),
    #{
        <<"data">> := Data
    } = Json,
    ?assert(length(Data) > 0),

    ok.

rewards_test(_Config) ->
    Validator = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/validators/",
            Validator,
            "/rewards?max_time=2021-07-27&min_time=2021-06-27"
        ]),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    case maps:get(<<"cursor">>, Json, undefined) of
        undefined ->
            ok;
        Cursor ->
            {ok, {_, _, CursorJson}} =
                ?json_request(["/v1/validators/", Validator, "/rewards?cursor=", Cursor]),
            #{<<"data">> := CursorData} = CursorJson,
            ?assert(length(CursorData) >= 0)
    end,
    ok.

rewards_all_sum_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/validators/",
            "rewards/sum?max_time=2021-07-27&min_time=2021-07-20"
        ]),
    #{<<"data">> := #{<<"sum">> := Sum}} = Json,
    ?assert(Sum >= 0),

    ok.

rewards_sum_test(_Config) ->
    Validator = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/validators/",
            Validator,
            "/rewards/sum?max_time=2021-07-27&min_time=2021-07-20"
        ]),
    #{<<"data">> := #{<<"sum">> := Sum}} = Json,
    ?assert(Sum >= 0),

    ok.

rewards_buckets_test(_Config) ->
    Validator = "11Q7Gmwq1fRe7pcEmBBtPatWWnyXrBtJLks65gGh89GLypbBaQW",
    {ok, {_, _, Json}} =
        ?json_request([
            "/v1/validators/",
            Validator,
            "/rewards/sum?max_time=2021-07-27&min_time=2021-07-20&bucket=day"
        ]),
    #{<<"data">> := Data} = Json,
    ?assertEqual(7, length(Data)),

    ok.

name_test(_Config) ->
    FetchName = "best-raisin-hare",
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/name/", FetchName]),
    #{
        <<"data">> := Results
    } = Json,
    ?assert(length(Results) >= 1),
    ok.

name_search_test(_Config) ->
    Search = "raisin",
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/name?search=", Search]),
    #{
        <<"data">> := Results
    } = Json,
    ?assert(length(Results) >= 1),
    ok.

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(["/v1/validators/stats"]),
    #{
        <<"data">> := #{
            <<"active">> := _,
            <<"staked">> := _,
            <<"unstaked">> := _
        }
    } = Json,
    ok.
