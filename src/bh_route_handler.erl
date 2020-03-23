-module(bh_route_handler).

-include("bh_route_handler.hrl").

-export([get_args/2,
         mk_response/1,
         lat_lon/2, lat_lon/3]).

-callback handle(elli:http_method(), Path::[binary()], Req::elli:req()) -> elli:result().

-type arg_spec() :: Key::atom() | {Key::atom(), Default::any()}.
-type arg() :: {Key::atom(), Value::any()}.

-spec get_args([arg_spec()], elli:req()) -> [arg()].
get_args(Names, Req) ->
    get_args(Names, Req, []).

get_args([], _Req, Acc) ->
    lists:reverse(Acc);
get_args([limit | Tail], Req, Acc) ->
    get_args([{limit, ?DEFAULT_ARG_LIMIT} | Tail], Req, Acc);
get_args([{limit, Default} | Tail], Req, Acc) ->
    V = elli_request:get_arg_decoded(<<"limit">>, Req, Default),
    try binary_to_integer(V) of
        N ->  get_args(Tail, Req, [{limit, min(?MAX_LIMIT, N)} | Acc])
    catch _:_ ->
            binary_to_integer(?DEFAULT_ARG_LIMIT)
    end;
get_args([Key | Tail], Req, Acc) when is_atom(Key) ->
    get_args([{Key, undefined} | Tail], Req, Acc);
get_args([{Key, Default} | Tail], Req, Acc) ->
    V = case elli_request:get_arg_decoded(atom_to_binary(Key, latin1), Req, Default) of
            <<>> -> Default;
            Arg -> Arg
        end,
    get_args(Tail, Req, [{Key, V} | Acc]).

mk_response({ok, Json}) ->
    {ok,
     [{<<"Content-Type">>, <<"application/json; charset=utf-8">>},
      {<<"Access-Control-Allow-Origin">>, <<"*">>}
     ],
     jiffy:encode(#{<<"data">> => Json}, [])};
mk_response({error, not_found}) ->
    ?RESPONSE_404.

lat_lon(Location, Fields) ->
    lat_lon(Location, {<<"lat">>, <<"lng">>}, Fields).

lat_lon(undefined, _, Fields) ->
    Fields;
lat_lon(null, _, Fields) ->
    Fields;
lat_lon(Location, {LatName, LonName}, Fields) when is_binary(Location) ->
    {Lat, Lon} = h3:to_geo(h3:from_string(binary_to_list(Location))),
    Fields#{
            LatName => Lat,
            LonName => Lon
           }.
