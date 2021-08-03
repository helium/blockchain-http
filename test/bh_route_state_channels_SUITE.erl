-module(bh_route_state_channels_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() ->
    [
        list_test,
        stats_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, FirstJson}} = ?json_request("/v1/state_channels"),
    #{
        <<"data">> := FirstTxns,
        <<"cursor">> := Cursor
    } = FirstJson,

    ?assert(length(FirstTxns) =< ?STATE_CHANNEL_TXN_LIST_LIMIT),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/state_channels?cursor=", Cursor]),
    #{
        <<"data">> := NextTxns,
        <<"cursor">> := _
    } = NextJson,
    ?assert(length(NextTxns) =< ?STATE_CHANNEL_TXN_LIST_LIMIT),

    ok.

stats_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/state_channels/stats"),
    #{<<"data">> := #{<<"last_day">> := Value}} = Json,
    ?assert(Value >= 0),
    ok.
