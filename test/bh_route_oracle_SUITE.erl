-module(bh_route_oracle_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        price_test,
        price_at_block_test,
        price_at_invalid_block_test,
        list_test,
        activity_list_test,
        price_predictions_test,
        price_stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

price_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices/current"),
    ?assertMatch(
        #{<<"data">> := #{<<"block">> := _, <<"price">> := _, <<"timestamp">> := _}},
        Json
    ),

    ok.

price_at_block_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices/366920"),
    ?assertMatch(
        #{<<"data">> := #{<<"block">> := _, <<"price">> := _, <<"timestamp">> := _}},
        Json
    ),

    ok.

price_at_invalid_block_test(_Config) ->
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/oracle/prices/not_int")),

    ok.

list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices"),
    #{<<"data">> := Data} = Json,
    ?assert(length(Data) >= 0),

    ok.

activity_list_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/oracle/activity"),
    #{<<"data">> := AllData} = AllJson,
    ?assert(length(AllData) >= 0),

    {ok, {_, _, OneJson}} = ?json_request(
        "/v1/oracle/13CFFcmPtMvNQCpWQRXCTqXPnXtcsibDWVwiQRKpUCt4nqtF7RE/activity"
    ),
    #{<<"data">> := OneData} = OneJson,
    ?assert(length(OneData) >= 0),

    ok.

price_predictions_test(_Config) ->
    {ok, {_, _, AllJson}} = ?json_request("/v1/oracle/predictions"),
    #{<<"data">> := AllData} = AllJson,
    ?assert(length(AllData) >= 0),

    ok.

price_stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/oracle/prices/stats?min_time=-30%20day"),
    #{<<"data">> := #{<<"max">> := Max}} = Json,
    ?assert(Max >= 0),
    ok.
