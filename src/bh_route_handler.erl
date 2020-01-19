-module(bh_route_handler).

-export([mk_response/1]).

-callback prepare_conn(epgsql:connection()) -> ok.
-callback handle(elli:http_method(), Path::[binary()], Req::elli:req()) -> elli:result().


mk_response({ok, Json}) ->
    {ok,
     [{<<"Content-Type">>, <<"application/json; charset=utf-8">>}],
     jsone:encode(#{<<"data">> => Json}, [undefined_as_null])}.
