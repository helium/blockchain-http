-module(bh_route_handler).

-include("bh_route_handler.hrl").

-export([get_args/2,
         mk_response/2,
         lat_lon/2, lat_lon/3,
         cursor_encode/1, cursor_decode/1]).

-callback handle(elli:http_method(), Path::[binary()], Req::elli:req()) -> elli:result().

-type arg_spec() :: Key::atom() | {Key::atom(), Default::any()}.
-type arg() :: {Key::atom(), Value::any()}.

-spec get_args([arg_spec()], elli:req()) -> [arg()].
get_args(Names, Req) ->
    get_args(Names, Req, []).

get_args([], _Req, Acc) ->
    lists:reverse(Acc);
get_args([Key | Tail], Req, Acc) when is_atom(Key) ->
    get_args([{Key, undefined} | Tail], Req, Acc);
get_args([{Key, Default} | Tail], Req, Acc) ->
    V = case elli_request:get_arg_decoded(atom_to_binary(Key, latin1), Req, Default) of
            <<>> -> Default;
            Arg -> Arg
        end,
    get_args(Tail, Req, [{Key, V} | Acc]).


-type cache_time() :: infinity
                    | never
                    | undefined
                    | block_time.

%% @doc Construct a standard response given a map, list and an
%% optional cursor if needed. Given an error tuple, it will respond
%% with a pre-configured error code.
-spec mk_response({ok, Json::(map() | list()), Cursor::map() | undefined}
                  | {ok, Json::(map() | list())}
                  | {error, term()},
                 cache_time()) -> {ok | elli:response_code(), elli:headers(), elli:body()}.
mk_response({ok, Json, Cursor}, CacheTime) ->
     Result0 = #{ data => Json },
     Result = case Cursor of
                  undefined ->
                      Result0;
                  _ ->
                      Result0#{ cursor => cursor_encode(Cursor)}
              end,
    {ok,
     add_cache_header(
       CacheTime,
       [{<<"Content-Type">>, <<"application/json; charset=utf-8">>},
        {<<"Access-Control-Allow-Origin">>, <<"*">>}
       ]),
     jiffy:encode(Result, [])};
mk_response({ok, Json}, CacheTime) ->
    mk_response({ok, Json, undefined}, CacheTime);
mk_response({error, badarg}, _) ->
    ?RESPONSE_400;
mk_response({error, conflict}, _) ->
    ?RESPONSE_409;
mk_response({error, not_found}, _) ->
    ?RESPONSE_404.


-spec add_cache_header(cache_time(), Acc::list()) -> list(tuple()).
add_cache_header(undefined, Acc) ->
    Acc;
add_cache_header(infinity, Acc) ->
    [{<<"Surrogate-Control">>, <<"max-age=86400">>},
     {<<"Cache-Control">>, <<"max-age=86400">>}
     | Acc];
add_cache_header(never, Acc) ->
     [{<<"Cache-Control">>, <<"private, no-store">>}
      | Acc];
add_cache_header(block_time, Acc) ->
    [{<<"Surrogate-Control">>, <<"max-age=60">>},
     {<<"Cache-Control">>, <<"max-age=60">>}
     | Acc].

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

-spec cursor_decode(binary()) -> {ok, map()} | {error, term()}.
cursor_decode(Bin) when is_binary(Bin) ->
    try  jiffy:decode(base64url:decode(Bin), [return_maps]) of
         Map when is_map(Map) ->
            {ok, Map};
         _ ->
            {error, badarg}
    catch _:_ ->
            {error, badarg}
    end;
cursor_decode(_) ->
    {error, badarg}.


-spec cursor_encode(map()) -> binary().
cursor_encode(Map) ->
    ?BIN_TO_B64(jiffy:encode(Map)).
