-module(bh_middleware_cors).

-export([postprocess/3]).

-define(RESPONSE_HEADER_CORS, {<<"Access-Control-Allow-Origin">>, <<"*">>}).

postprocess(_Req, {Code, Body}, _) ->
    {Code, [?RESPONSE_HEADER_CORS], Body};
postprocess(_Req, {Code, Headers, Body}, _) ->
    {Code, [?RESPONSE_HEADER_CORS | Headers], Body}.
