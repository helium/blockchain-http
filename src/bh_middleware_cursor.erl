-module(bh_middleware_cursor).

-export([handle/2]).

-spec handle(elli:req(), elli:config()) -> elli_handler:result().
handle(Req, _Config) ->
    Args = elli_request:get_args(Req),
    case lists:keyfind(<<"cursor">>, 1, Args) of
        false ->
            ignore;
        {<<"cursor">>, _Cursor} when length(Args) == 1 -> ignore;
        {<<"cursor">>, Cursor} ->
            Location = [
                <<"/">>,
                lists:join(<<"/">>, elli_request:path(Req)),
                <<"?cursor=">>,
                Cursor
            ],
            {301, [{<<"Location">>, Location}], <<>>}
    end.
