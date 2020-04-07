-module(ct_utils).
-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").

init_bh(Config) ->
    application:ensure_all_started(lager),
    application:ensure_all_started(dispcount),
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
    httpc:request(get, {"http://localhost:8080" ++ Path, []}, [], [{body_format, binary}]).

json_request(Path) ->
    case ?MODULE:request(Path) of
        {ok, {Status={_, 200, _}, Headers, Body}} ->
            Json = jiffy:decode(Body, [return_maps]),
            {ok, {Status, Headers, Json}};
        {ok, {Status, _Headers, _Body}} ->
            {error, Status};
        {error, Error} ->
            {error, Error}
    end.
