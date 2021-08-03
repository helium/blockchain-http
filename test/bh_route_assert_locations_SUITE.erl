-module(bh_route_assert_locations_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() ->
    [
        list_test
    ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, FirstJson}} = ?json_request("/v1/assert_locations"),
    #{
        <<"data">> := FirstTxns,
        <<"cursor">> := Cursor
    } = FirstJson,

    ?assert(length(FirstTxns) =< ?TXN_LIST_LIMIT),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/assert_locations?cursor=", Cursor]),
    #{
        <<"data">> := NextTxns,
        <<"cursor">> := _
    } = NextJson,
    ?assert(length(NextTxns) =< ?TXN_LIST_LIMIT),

    ok.
