-module(bh_route_handler).

-include("bh_route_handler.hrl").

-export([get_args/2,
         mk_response/1,
         lat_lon/2, lat_lon/3]).

-callback handle(elli:http_method(), Path::[binary()], Req::elli:req()) -> elli:result().

get_args(Names, Req) ->
    get_args(Names, Req, []).

get_args([], _Req, Acc) ->
    lists:reverse(Acc);
get_args([limit | Tail], Req, Acc) ->
    get_args([{limit, ?DEFAULT_ARG_LIMIT} | Tail], Req, Acc);
get_args([{limit, Default} | Tail], Req, Acc) ->
    V = elli_request:get_arg_decoded(<<"limit">>, Req, Default),
    get_args(Tail, Req, [{limit, min(?MAX_LIMIT, binary_to_integer(V))} | Acc]);
get_args([Key | Tail], Req, Acc) when is_atom(Key) ->
    get_args([{Key, undefined} | Tail], Req, Acc);
get_args([{Key, Default} | Tail], Req, Acc) ->
    V = elli_request:get_arg_decoded(atom_to_binary(Key, latin1), Req, Default),
    get_args(Tail, Req, [{Key, V} | Acc]).

mk_response({ok, Json}) ->
    {ok,
     [{<<"Content-Type">>, <<"application/json; charset=utf-8">>}],
     jsone:encode(#{<<"data">> => Json}, [undefined_as_null])};
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
