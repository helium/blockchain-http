-module(bh_route_versions).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities

prepare_conn(_Conn) ->
    #{}.

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_versions(), {block_time, 60});
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_versions() ->
    {ok, HttpVersion} = application:get_key(blockchain_http, vsn),
    Versions = #{
        blockchain_http => list_to_binary(HttpVersion)
    },
    {ok, Versions}.
