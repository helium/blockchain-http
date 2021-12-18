-module(bh_route_txns_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        get_test,
        get_actor_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

get_test(_Config) ->
    TxnHash = "DmTG-Nbp6rLr4ERjqOvBcDC-94G4qsWg-Ii9BeL2qhY",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/transactions/",
        TxnHash
    ]),
    ?assertMatch(#{<<"data">> := _}, Json),
    ok.

get_actor_test(_Config) ->
    TxnHash = "ks_-16PtsDQo7zWgdFoucKw4AA4pj5dvUzv17gSdU0o",
    Actor = "14fkPiFHh6oqecKhkjRWTtcJByy44t5S68SZQG6QExtStSQAZsr",
    {ok, {_, _, Json}} = ?json_request([
        "/v1/transactions/",
        TxnHash,
        "?actor=",
        Actor
    ]),
    #{
        <<"data">> := #{
            <<"rewards">> := Rewards
        }
    } = Json,
    ?assertMatch(
        [
            #{
                <<"account">> :=
                    <<"14fkPiFHh6oqecKhkjRWTtcJByy44t5S68SZQG6QExtStSQAZsr">>,
                <<"gateway">> :=
                    <<"112jbxsCXERvuu6Xq3cjJUBpoKSwLsUuUGNF5wgh8cgj8Vsf5guV">>,
                <<"amount">> := _,
                <<"type">> := <<"poc_witnesses">>
            }
        ],
        Rewards
    ),
    ok.
