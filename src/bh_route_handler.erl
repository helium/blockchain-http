-module(bh_route_handler).

-callback prepare_conn(epgsql:connection()) -> ok.
-callback handle(elli:http_method(), Path::[binary()], Req::elli:req()) -> elli:result().
