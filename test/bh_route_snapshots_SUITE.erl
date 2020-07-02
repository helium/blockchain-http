-module(bh_route_snapshots_SUITE).
-compile([nowarn_export_all, export_all]).

-include("ct_utils.hrl").
-include("../src/bh_route_handler.hrl").

all() -> [
          list_test,
          current_test
         ].

init_per_suite(Config) ->
    ?init_bh(Config).

end_per_suite(Config) ->
    ?end_bh(Config).

list_test(_Config) ->
    {ok, {_, _, FirstJson}} = ?json_request("/v1/snapshots"),
    #{ <<"data">> := FirstTxns,
       <<"cursor">> := Cursor
     } = FirstJson,

    ?assert(length(FirstTxns) =< ?SNAPSHOT_LIST_LIMIT),

    {ok, {_, _, NextJson}} = ?json_request(["/v1/snapshots?cursor=", Cursor]),
    #{ <<"data">> := NextTxns
     } = NextJson,
    ?assert(length(NextTxns) =< ?SNAPSHOT_LIST_LIMIT).

current_test(_Config) ->
    {ok, {_, _, Json}} = ?json_request("/v1/snapshots/current"),
    ?assertMatch(#{ <<"data">> :=
                        #{ <<"block">> :=  _,
                           <<"snapshot_hash">> := _
                         }}, Json).

