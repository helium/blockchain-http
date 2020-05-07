-module(bh_route_stats_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() -> [
          stats_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/stats"),
    #{ <<"data">> := #{
                       <<"block_times">> := _ ,
                       <<"election_times">> := _,
                       <<"token_supply">> := _
                      }
     } = Json,
    ok.
