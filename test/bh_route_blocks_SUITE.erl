-module(bh_route_blocks_SUITE).
-compile([nowarn_export_all, export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [
          height_test
         ].

init_per_testcase(_, Config) ->
    application:ensure_all_started(dispcount),
    {ok, _} = bh_sup:start_link(),
    Config.

end_per_testcase(_, Config) ->
    Config.

request(_Config, Path) ->
    httpc:request(get, {"http://localhost:8080" ++ Path, []}, [], [{body_format, binary}]).

json_request(Config, Path) ->
    case request(Config, Path) of
        {ok, {StatusLine, Headers, Body}} ->
            Json = jiffy:decode(Body, [return_maps]),
            {ok, {StatusLine, Headers, Json}};
        {error, Error} ->
            {error, Error}
    end.

height_test(Config) ->
    {ok, {_, _, Json}} = json_request(Config, "/v1/blocks/height"),
    ?assertMatch(#{ <<"data">> := #{ <<"height">> :=  _}}, Json),

    ok.
