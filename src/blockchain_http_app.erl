-module(blockchain_http_app).

-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

start(_StartType, _StartArgs) ->
    bh_sup:start_link().

prep_stop(State) ->
    persistent_term:put(ro_pool, shutdown),
    persistent_term:put(rw_pool, shutdown),
    State.

stop(_State) ->
    ok.
