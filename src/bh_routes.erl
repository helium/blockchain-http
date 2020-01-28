-module(bh_routes).

-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

handle(Method, [<<"v1">>, <<"blocks">> | Tail], Req) ->
    bh_route_blocks:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"accounts">> | Tail], Req) ->
    bh_route_accounts:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"hotspots">> | Tail], Req) ->
    bh_route_hotspots:handle(Method, Tail, Req);

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.

%% @doc Handle request events, like request completed, exception
%% thrown, client timeout, etc. Must return `ok'.
handle_event(_Event, _Data, _Args) ->
    ok.
