-module(bh_middleware_timespan).

-include("bh_route_handler.hrl").

-export([handle/2]).

-spec handle(elli:req(), elli:config()) -> elli_handler:result().
handle(Req, _Config) ->
    case ?GET_ARGS([max_time, min_time], Req) of
        [{max_time, undefined}, {min_time, undefined}] ->
            ignore;
        [{max_time, MaxTime0}, {min_time, MinTime0}] ->
            case ?PARSE_TIMESPAN(MaxTime0, MinTime0) of
                {error, _} ->
                    ?RESPONSE_400;
                {ok, {MaxTime, MinTime}} ->
                    BinMaxTime = iso8601:format(MaxTime),
                    BinMinTime = iso8601:format(MinTime),
                    case BinMinTime == MinTime0 andalso BinMaxTime == MaxTime0 of
                        true ->
                            ignore;
                        false ->
                            Args = elli_request:get_args(Req),
                            NewArgs = lists:foldl(
                                fun
                                    ({min_time, NewMinTime}, Acc) ->
                                        lists:keystore(
                                            <<"min_time">>, 1, Acc, {<<"min_time">>, NewMinTime}
                                        );
                                    ({max_time, NewMaxTime}, Acc) ->
                                        lists:keystore(
                                            <<"max_time">>, 1, Acc, {<<"max_time">>, NewMaxTime}
                                        )
                                end,
                                Args,
                                [{min_time, BinMinTime}, {max_time, BinMaxTime}]
                            ),
                            Location = [
                                <<"/">>,
                                lists:join(<<"/">>, elli_request:path(Req)),
                                <<"?">>,
                                uri_string:compose_query(NewArgs)
                            ],
                            {302, [{<<"Location">>, Location}], <<>>}
                    end
            end
    end.
