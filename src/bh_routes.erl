-module(bh_routes).

-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").

-include("bh_route_handler.hrl").

-behaviour(elli_handler).

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

handle(Method, [<<"v1">>, <<"stats">> | Tail], Req) ->
    bh_route_stats:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"blocks">> | Tail], Req) ->
    bh_route_blocks:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"accounts">> | Tail], Req) ->
    bh_route_accounts:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"hotspots">> | Tail], Req) ->
    bh_route_hotspots:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"transactions">> | Tail], Req) ->
    bh_route_txns:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"pending_transactions">> | Tail], Req) ->
    bh_route_pending_txns:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"elections">> | Tail], Req) ->
    bh_route_elections:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"challenges">> | Tail], Req) ->
    bh_route_challenges:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"oracle">> | Tail], Req) ->
    bh_route_oracle:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"vars">> | Tail], Req) ->
    bh_route_vars:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"snapshots">> | Tail], Req) ->
    bh_route_snapshots:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"cities">> | Tail], Req) ->
    bh_route_cities:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"ouis">> | Tail], Req) ->
    bh_route_ouis:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"locations">> | Tail], Req) ->
    bh_route_locations:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"rewards">> | Tail], Req) ->
    bh_route_rewards:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"dc_burns">> | Tail], Req) ->
    bh_route_dc_burns:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"state_channels">> | Tail], Req) ->
    bh_route_state_channels:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"assert_locations">> | Tail], Req) ->
    bh_route_assert_locations:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"validators">> | Tail], Req) ->
    bh_route_validators:handle(Method, Tail, Req);
handle('GET', [], _Req) ->
    {200, [], <<>>};
handle(_, _, _Req) ->
    ?RESPONSE_404.

handle_event(request_throw, [Req, Exception, Stack], _Config) ->
    lager:error(
        "exception: ~p~nstack: ~p~nrequest: ~p~n",
        [Exception, Stack, elli_request:to_proplist(Req)]
    ),
    ok;
handle_event(request_exit, [Req, Exit, Stack], _Config) ->
    lager:error(
        "exit: ~p~nstack: ~p~nrequest: ~p~n",
        [Exit, Stack, elli_request:to_proplist(Req)]
    ),
    ok;
handle_event(request_error, [Req, Error, Stack], _Config) ->
    lager:error(
        "error: ~p~nstack: ~p~nrequest: ~p~n",
        [Error, Stack, elli_request:to_proplist(Req)]
    ),
    ok;
handle_event(_, _, _) ->
    ok.
