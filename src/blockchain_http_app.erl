-module(blockchain_http_app).

-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    bh_sup:start_link().

stop(_State) ->
    ok.
