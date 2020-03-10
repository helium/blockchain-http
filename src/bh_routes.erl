-module(bh_routes).

-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-include("bh_route_handler.hrl").

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
handle(Method, [<<"v1">>, <<"transactions">> | Tail], Req) ->
    bh_route_txns:handle(Method, Tail, Req);
handle(Method, [<<"v1">>, <<"pending_transactions">> | Tail], Req) ->
    bh_route_pending_txns:handle(Method, Tail, Req);
handle('GET', [], _Req) ->
    {200, [], <<>>};

handle(_, _, _Req) ->
    ?RESPONSE_404.

%% @doc Handle request events, like request completed, exception
%% thrown, client timeout, etc. Must return `ok'.
%%
%% Copied from:
%% https://github.com/elli-lib/elli_access_log/blob/master/src/elli_access_log.erl
%%
%% Sends a simple log line for every request, even errors, to
%% lager. The line includes the following timings, all specified in
%% wall-clock microseconds:
%%
%% RequestLine/Headers/Body/User/Response/Total
%%
%% * RequestLine: time between accept returning and complete receive
%%   of the request line, ie. "GET /foo HTTP/1.1". If keep-alive is
%%   used, this will be the time since the initial accept so it might
%%   be very high.
%%
%% * Headers: Time to receive all headers
%%
%% * Body: Time to receive the entire body into memory, not including
%%   any decoding
%%
%% * User: Time spent in the callback. If middleware is used, the
%%   runtime of the middleware is included in this number
%%
%% * Response: Time taken to send the response to the client
%%
%% * Total: The time between the request line was received and the
%%   response was sent. This is as close we can get to the actual time
%%   of the request as seen by the user.
handle_event(request_complete, [Req, ResponseCode, _ResponseHeaders,
                                _ResponseBody, {Timings, Sizes}], _Config) ->

    %% The Elli request process is done handling the request, so we
    %% can afford to do some heavy lifting here.

    case elli_request:raw_path(Req) of
        <<"/">> ->
            %% Ignore heartbeat requests
            ok;
        Path ->
            Accepted     = proplists:get_value(accepted, Timings),
            RequestStart = proplists:get_value(request_start, Timings),
            HeadersEnd   = proplists:get_value(headers_end, Timings),
            BodyEnd      = proplists:get_value(body_end, Timings),
            UserStart    = proplists:get_value(user_start, Timings),
            UserEnd      = proplists:get_value(user_end, Timings),
            RequestEnd   = proplists:get_value(request_end, Timings),
            BodySize = proplists:get_value(resp_body, Sizes),

            TimeStr = io_lib:format("~w/~w/~w/~w/~w/~w",
                                    [RequestStart - Accepted,
                                     HeadersEnd -  RequestStart,
                                     BodyEnd - HeadersEnd,
                                     UserEnd - UserStart,
                                     RequestEnd - UserEnd,
                                     RequestEnd - RequestStart]),

            lager:info("~s ~s ~w ~w \"~s ~s\"",
                       [elli_request:peer(Req),
                        TimeStr,
                        ResponseCode,
                        BodySize,
                        elli_request:method(Req),
                        Path
                       ]),
            ok
    end;
handle_event(chunk_complete, [Req, ResponseCode, ResponseHeaders,
                              _ClosingEnd, Timings], Config) ->
    handle_event(request_complete, [Req, ResponseCode, ResponseHeaders,
                                    <<>>, Timings], Config);
handle_event(request_throw, [Req, Exception, Stack], _Config) ->
    error_logger:error_msg("exception: ~p~nstack: ~p~nrequest: ~p~n",
                           [Exception, Stack, elli_request:to_proplist(Req)]),
    ok;
handle_event(request_exit, [Req, Exit, Stack], _Config) ->
    error_logger:error_msg("exit: ~p~nstack: ~p~nrequest: ~p~n",
                           [Exit, Stack, elli_request:to_proplist(Req)]),
    ok;
handle_event(request_error, [Req, Error, Stack], _Config) ->
    error_logger:error_msg("error: ~p~nstack: ~p~nrequest: ~p~n",
                           [Error, Stack, elli_request:to_proplist(Req)]),
    ok;

handle_event(_, _, _) ->
    ok.
