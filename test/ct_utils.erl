-module(ct_utils).

-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").

init_bh(Config) ->
    application:ensure_all_started(lager),
    application:ensure_all_started(dispcount),
    application:ensure_all_started(throttle),
    {ok, Pid} = bh_sup:start_link(),
    unlink(Pid),
    [{bh_sup, Pid} | Config].

end_bh(Config) ->
    Sup = ?config(bh_sup, Config),
    gen_server:stop(Sup),
    dispcount:stop_dispatch(ro_pool),
    dispcount:stop_dispatch(rw_pool),
    Config.

request(Path) ->
    httpc:request(get, {lists:flatten(["http://localhost:8080", Path]), []}, [], [
        {body_format, binary}
    ]).

json_request(Path) ->
    case ?MODULE:request(Path) of
        {ok, {Status = {_, 200, _}, Headers, Body}} ->
            Json = jiffy:decode(Body, [return_maps]),
            {ok, {Status, Headers, Json}};
        {ok, {Status, _Headers, _Body}} ->
            {error, Status};
        {error, Error} ->
            {error, Error}
    end.

fold_json_request(Fun, Base, {ok, {_, _, Json}}, Acc) ->
    NewAcc = lists:foldl(Fun, Acc, maps:get(<<"data">>, Json)),
    case maps:get(<<"cursor">>, Json, undefined) of
        undefined ->
            NewAcc;
        Cursor ->
            fold_json_request(Fun, Base, json_request([Base, "?cursor=", Cursor]), NewAcc)
    end.
