-module(bh_route_blocks_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() -> [
          height_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

height_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/height"),
    ?assertMatch(#{ <<"data">> := #{ <<"height">> :=  _}}, Json),

    ok.
