-module(bh_route_vars_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        name_test,
        list_test,
        list_named_test,
        activity_list_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

name_test(_Config) ->
    % integer
    Names = [
        "poc_version",
        % float
        "securities_percent",
        % atom
        "predicate_callback_mod",
        % oracle keys
        "price_oracle_public_keys"
    ],
    lists:foreach(
        fun(Name) ->
            {ok, {_, _, Json}} = ?json_request(["/v1/vars/", Name]),
            ?assertMatch(#{<<"data">> := _}, Json)
        end,
        Names
    ),
    ok.

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/vars"),
    #{<<"data">> := Data} = Json,
    ?assert(map_size(Data) >= 0),

    ok.

list_named_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/vars?keys=poc_version,securities_percent"),
    #{<<"data">> := Data} = Json,
    ?assertEqual(2, map_size(Data)),

    ok.

activity_list_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/vars/activity"),
    #{<<"data">> := AllData} = AllJson,
    ?assert(length(AllData) >= 0),

    ok.
