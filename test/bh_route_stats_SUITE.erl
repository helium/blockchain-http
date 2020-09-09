-module(bh_route_stats_SUITE).

-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() ->
    [
        stats_test,
        token_supply_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/stats"),
    #{
        <<"data">> := #{
            <<"block_times">> := _,
            <<"election_times">> := _,
            <<"token_supply">> := _
        }
    } = Json,
    ok.

token_supply_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/stats/token_supply"),
    #{<<"data">> := #{<<"token_supply">> := _}} = Json,

    {ok, {_, _, Value}} = ?json_request("/v1/stats/token_supply?format=raw"),
    ?assert(Value >= 0),
    ok.
