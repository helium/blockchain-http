-module(bh_route_rewards_SUITE).

-compile([nowarn_export_all, export_all]).

-include("bh_route_handler.hrl").
-include("ct_utils.hrl").

all() ->
    [
        rewards_sum_test,
        rewards_buckets_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

rewards_sum_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request(["/v1/rewards/sum?min_time=-2%20day"]),
    #{<<"data">> := #{<<"sum">> := Sum}} = Json,
    ?assert(Sum >= 0),

    ok.

rewards_buckets_test(_Config) ->
    {ok, {_, _, Json}} =
        ?json_request(["/v1/rewards/sum?min_time=-7%20day&bucket=day"]),
    #{<<"data">> := Data} = Json,
    ?assertEqual(7, length(Data)),

    ok.
