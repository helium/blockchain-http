-module(bh_middleware_throttle).

-export([handle/2, postprocess/3, handle_event/3]).

-behaviour(elli_handler).

-include("bh_route_handler.hrl").

%%
%% ELLI
%%

handle(Req, _Args) ->
    Host = get_actor(Req),
    case throttle:check(request_count, Host) of
        {limit_exceeded, 0, TimeToResetInMs} ->
            ?RESPONSE_429(TimeToResetInMs);
        _ ->
            case throttle:peek(request_time, Host) of
                {limit_exceeded, 0, TimeToResetInMs} ->
                    ?RESPONSE_429(TimeToResetInMs);
                _ ->
                    ignore
            end
    end.

postprocess(Req, {Response, Headers, Body}, #{debug := true}) ->
    {_, Remainder, _} = throttle:peek(request_time, get_actor(Req)),
    {Response, [{<<"X-Request-Time-Remaining">>, integer_to_list(Remainder)} | Headers], Body};
postprocess(_Req, Res, _Args) ->
    Res.

%%
%% ELLI EVENT CALLBACKS
%%

handle_event(request_complete, [Req, Code, _Hs, _B, {Timings, _Sizes}], Args) ->
    RequestStart = proplists:get_value(request_start, Timings),
    RequestEnd = proplists:get_value(request_end, Timings),
    GraceTime = maps:get(grace_time, Args, 0),
    Duration = max(1, ((RequestEnd - RequestStart) div 1000000) - GraceTime),
    Actor = get_actor(Req),
    throttle:update(request_time, Actor, Duration),
    case Duration > maps:get(log_longer_than, Args, infinity) of
        true ->
            lager:notice("~s request for ~s from ~s with result ~p took ~p ms", [
                elli_request:method(Req),
                elli_request:raw_path(Req),
                Actor,
                Code,
                Duration + GraceTime
            ]);
        false ->
            ok
    end,
    ok;
handle_event(elli_startup, _Args, #{request_time := MS, request_interval := Interval, request_count := Count}) ->
    throttle:setup(request_time, MS, Interval),
    throttle:setup(request_count, Count, Interval);
handle_event(_Event, _Data, _Args) ->
    ok.

%% internal functions

get_actor(Req) ->
    case lists:keyfind(<<"X-Forwarded-For">>, 1, elli_request:headers(Req)) of
        false ->
            elli_request:peer(Req);
        {<<"X-Forwarded-For">>, Value} ->
            hd(binary:split(Value, <<",">>))
    end.
