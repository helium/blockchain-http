-module(bh_route_challenges_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() -> [
          challenge_list_test,
          hotspot_challenge_list_test,
          account_challenge_list_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

challenge_list_test(_Config) ->
    {ok, {_, _, FirstJson}} = ?json_request("/v1/challenges"),
    #{ <<"data">> := FirstTxns,
       <<"cursor">> := Cursor,
       <<"meta">> := #{ <<"start_block">> := StartBlock,
                        <<"end_block">> := EndBlock}
     } = FirstJson,

    ?assert(length(FirstTxns) >= 0),
    ?assert(StartBlock - EndBlock =< ?CHALLENGE_LIST_BLOCK_LIMIT),

    {ok, {_, _, NextJson}} = ?json_request("/v1/challenges?cursor=" ++ binary_to_list(Cursor)),
    #{ <<"data">> := NextTxns,
       <<"cursor">> := _ ,
       <<"meta">> := #{ <<"start_block">> := NextStartBlock,
                        <<"end_block">> := NextEndBlock
                      }
     } = NextJson,
    ?assert(length(NextTxns)  >= 0),
    ?assert(NextStartBlock - NextEndBlock =< ?CHALLENGE_LIST_BLOCK_LIMIT),
    ?assertEqual(NextStartBlock, EndBlock),

    ok.

hotspot_challenge_list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/hotspots/1182nyT3oXZPMztMSww4mzaaQXGXd5T7JwDfEth6obSCwwxxfsB/challenges"),
    #{ <<"data">> := Txns } = Json,
    ?assert(length(Txns) >= 0).

account_challenge_list_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/accounts/13GCcF7oGb6waFBzYDMmydmXx4vNDUZGX4LE3QUh8eSBG53s5bx/challenges"),
    #{ <<"data">> := Txns } = Json,
    ?assert(length(Txns) >= 0).
