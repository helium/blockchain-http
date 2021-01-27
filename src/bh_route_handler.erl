-module(bh_route_handler).

-include("bh_route_handler.hrl").

-export([
    get_args/2,
    parse_timespan/2,
    parse_bucket/1,
    parse_interval/1,
    parse_interval/2,
    parse_bucketed_timespan/3,
    mk_response/2,
    add_cache_header/2,
    lat_lon/2,
    lat_lon/3,
    cursor_encode/1,
    cursor_decode/1
]).

-callback handle(elli:http_method(), Path :: [binary()], Req :: elli:req()) ->
    elli:result().

-type arg_spec() :: Key :: atom() | {Key :: atom(), Default :: any()}.
-type arg() :: {Key :: atom(), Value :: any()}.
-type interval_spec() :: {Type :: binary(), Spec :: epgsql:pg_interval()}.
-type timespan() :: {Max :: calendar:datetime(), Min :: calendar:datetime()}.
-type blockspan() :: {MaxBlock :: non_neg_integer(), Min :: non_neg_integer()}.
-type cache_time() ::
    infinity |
    never |
    undefined |
    block_time |
    {block_time, pos_integer()}.

-export_type([cache_time/0, timespan/0, blockspan/0, interval_spec/0]).

-spec get_args([arg_spec()], elli:req()) -> [arg()].
get_args(Names, Req) ->
    get_args(Names, Req, []).

get_args([], _Req, Acc) ->
    lists:reverse(Acc);
get_args([Key | Tail], Req, Acc) when is_atom(Key) ->
    get_args([{Key, undefined} | Tail], Req, Acc);
get_args([{Key, Default} | Tail], Req, Acc) ->
    V =
        case elli_request:get_arg_decoded(atom_to_binary(Key, latin1), Req, Default) of
            <<>> -> Default;
            Arg -> Arg
        end,
    get_args(Tail, Req, [{Key, V} | Acc]).

-spec parse_timespan(High :: binary(), Low :: binary()) ->
    {ok, timespan()} | {error, term()}.
parse_timespan(MaxTime0, MinTime0) ->
    ParseTime = fun
        (<<"now">>) -> calendar:universal_time();
        (undefined) -> calendar:universal_time();
        (T) -> iso8601:parse(T)
    end,
    Validate = fun (Max, Min) ->
        calendar:datetime_to_gregorian_seconds(Max) >
            calendar:datetime_to_gregorian_seconds(Min)
    end,
    try
        {MaxTime, MinTime} = {ParseTime(MaxTime0), ParseTime(MinTime0)},
        case Validate(MaxTime, MinTime) of
            true -> {ok, {MaxTime, MinTime}};
            false -> {error, badarg}
        end
    catch
        error:badarg ->
            {error, badarg}
    end.

-spec parse_bucket(binary()) -> {ok, interval_spec()} | {error, term()}.
parse_bucket(Bin) ->
    parse_interval(1, Bin).

-spec parse_interval(binary()) -> {ok, interval_spec()} | {error, term()}.
parse_interval(Bin) ->
    TrimmedBin = string:trim(Bin),
    [NumBin, Bucket] = string:split(TrimmedBin, " ", leading),
    try
        Num = binary_to_integer(NumBin),
        parse_interval(Num, string:trim(Bucket))
    catch
        error:badarg ->
            {error, badarg}
    end.

parse_interval(N, <<"month">>) ->
    {ok, {<<"month">>, {{0, 0, 0}, 0, N}}};
parse_interval(N, <<"week">>) ->
    {ok, {<<"week">>, {{0, 0, 0}, N * 7, 0}}};
parse_interval(N, <<"day">>) ->
    {ok, {<<"day">>, {{0, 0, 0}, N, 0}}};
parse_interval(N, <<"hour">>) ->
    {ok, {<<"hour">>, {{N, 0, 0}, 0, 0}}};
parse_interval(_, _) ->
    {error, badarg}.

-spec parse_bucketed_timespan(High :: binary(), Low :: binary(), Step :: binary()) ->
    {ok, {timespan(), interval_spec()}} |
    {error, term()}.
parse_bucketed_timespan(MaxTime0, MinTime0, Bucket0) ->
    case parse_timespan(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            case parse_bucket(Bucket0) of
                {ok, Bucket} -> {ok, {{MaxTime, MinTime}, Bucket}};
                {error, Error} -> {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Construct a standard response given a map, list and an
%% optional cursor if needed. Given an error tuple, it will respond
%% with a pre-configured error code.
-spec mk_response(
    {ok, Json :: (map() | list()), Cursor :: map() | undefined, Meta :: map | undefined} |
    {ok, Json :: (map() | list()), Cursor :: map() | undefined} |
    {ok, Json :: (map() | list())} |
    {error, term()},
    cache_time()
) ->
    {ok | elli:response_code(), elli:headers(), elli:body()}.
mk_response({ok, Json, Cursor, Meta}, CacheTime) ->
    Result0 = #{data => Json},
    Result1 =
        case Cursor of
            undefined -> Result0;
            _ -> Result0#{cursor => cursor_encode(Cursor)}
        end,
    Result =
        case Meta of
            undefined -> Result1;
            _ -> Result1#{meta => Meta}
        end,
    {ok,
        add_cache_header(
            CacheTime,
            [{<<"Content-Type">>, <<"application/json; charset=utf-8">>}]
        ),
        jiffy:encode(Result, [])};
mk_response({ok, Json}, CacheTime) ->
    mk_response({ok, Json, undefined, undefined}, CacheTime);
mk_response({ok, Json, Cursor}, CacheTime) ->
    mk_response({ok, Json, Cursor, undefined}, CacheTime);
mk_response({error, badarg}, _) ->
    ?RESPONSE_400;
mk_response({error, cursor_expired}, _) ->
    ?RESPONSE_400("Cursor Expired");
mk_response({error, conflict}, _) ->
    ?RESPONSE_409;
mk_response({error, not_found}, _) ->
    ?RESPONSE_404.

-spec add_cache_header(cache_time(), Acc :: list()) -> list(tuple()).
add_cache_header(undefined, Acc) ->
    Acc;
add_cache_header(infinity, Acc) ->
    [
        {<<"Surrogate-Control">>, <<"max-age=86400">>},
        {<<"Cache-Control">>, <<"max-age=86400">>}
        | Acc
    ];
add_cache_header(never, Acc) ->
    [{<<"Cache-Control">>, <<"private, no-store">>} | Acc];
add_cache_header(block_time, Acc) ->
    add_cache_header({block_time, 1}, Acc);
add_cache_header({block_time, N}, Acc) ->
    CacheTime = integer_to_binary(N * 60),
    [
        {<<"Surrogate-Control">>, <<"max-age=", CacheTime/binary>>},
        {<<"Cache-Control">>, <<"max-age=", CacheTime/binary>>}
        | Acc
    ].

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
    try jiffy:decode(base64url:decode(Bin), [return_maps]) of
        Map when is_map(Map) ->
            {ok, Map};
        _ ->
            {error, badarg}
    catch
        _:_ ->
            {error, badarg}
    end;
cursor_decode(_) ->
    {error, badarg}.

-spec cursor_encode(map()) -> binary().
cursor_encode(Map) ->
    ?BIN_TO_B64(jiffy:encode(Map)).
