-module(bh_route_blocks_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").

all() ->
    [
        height_test,
        height_by_time_test,
        block_for_height_test,
        block_for_invalid_height_test,
        block_for_height_txns_test,
        block_for_invalid_height_txns_test,
        block_for_height_txns_cursor_test,
        block_for_hash_test,
        block_for_hash_txns_test,
        block_for_invalid_hash_txns_test,
        block_stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

height_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/height"),
    ?assertMatch(#{<<"data">> := #{<<"height">> := _}}, Json),

    ok.

height_by_time_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/height?max_time=2021-04-20"),
    ?assertMatch(#{<<"data">> := #{<<"height">> := _}}, Json),

    ok.

block_for_height_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/1"),
    ?assertMatch(
        #{
            <<"data">> :=
                #{
                    <<"height">> := 1,
                    <<"transaction_count">> := 70
                }
        },
        Json
    ).

block_for_invalid_height_test(_Config) ->
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/blocks/not_int")),
    ok.

block_for_height_txns_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/1/transactions"),
    #{<<"data">> := Txns, <<"cursor">> := _} = Json,
    ?assertEqual(50, length(Txns)).

block_for_height_txns_cursor_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request([
        "/v1/blocks/1109085/transactions",
        "?cursor=eyJoYXNoIjoianhRX3BPakdQSGFoTWk5dzBPNW1oZDJ3WGlGTUp3Q3NsNnhyeDNvYTFHUSJ9"
    ]),
    #{<<"data">> := Txns, <<"cursor">> := _} = Json,
    ?assertEqual(50, length(Txns)).

block_for_invalid_height_txns_test(_Config) ->
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/blocks/0/transactions")),
    ?assertMatch({error, {_, 400, _}}, ?json_request("/v1/blocks/not_int/transactions")),
    ok.

block_for_hash_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(
        "/v1/blocks/hash/La6PuV80Ps9qTP0339Pwm64q3_deMTkv6JOo1251EJI"
    ),
    ?assertMatch(
        #{
            <<"data">> :=
                #{
                    <<"height">> := 1,
                    <<"transaction_count">> := 70
                }
        },
        Json
    ).

block_for_hash_txns_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request(
        "/v1/blocks/hash/La6PuV80Ps9qTP0339Pwm64q3_deMTkv6JOo1251EJI/transactions"
    ),
    #{<<"data">> := Txns, <<"cursor">> := _} = Json,
    ?assertEqual(50, length(Txns)).

block_for_invalid_hash_txns_test(_Config) ->
    ?assertMatch({error, {_, 404, _}}, ?json_request("/v1/blocks/hash/no_such_hash/transactions")),
    ok.

block_stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/blocks/stats"),
    #{<<"data">> := Stats} = Json,
    ?assert(maps:size(Stats) > 0),

    ok.
