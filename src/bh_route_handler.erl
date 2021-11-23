-module(bh_route_handler).

-include("bh_route_handler.hrl").

-export([
    get_args/2,
    parse_timestamp/1,
    parse_timespan/2,
    parse_bucket/2,
    parse_interval/1,
    parse_interval/2,
    parse_bucketed_timespan/3,
    mk_response/2,
    add_cache_header/2,
    lat_lon/2,
    lat_lon/3,
    insert_location_hex/2,
    insert_location_hex/3,
    cursor_encode/1,
    cursor_decode/1,
    parse_float/1,
    parse_int/1,
    try_or_else/3,
    filter_types_to_list/2,
    filter_types_to_sql/2,
    hotspot_modes_to_list/2,
    hotspot_modes_to_sql/2
]).

-callback handle(elli:http_method(), Path :: [binary()], Req :: elli:req()) -> elli:result().

-type arg_spec() :: Key :: atom() | {Key :: atom(), Default :: any()}.
-type arg() :: {Key :: atom(), Value :: any()}.
-type interval_spec() :: {Type :: binary(), Spec :: epgsql:pg_interval()}.
-type timespan() :: {Max :: calendar:datetime(), Min :: calendar:datetime()}.
-type blockspan() :: {MaxBlock :: non_neg_integer(), Min :: non_neg_integer()}.
-type cache_time() ::
    infinity
    | never
    | undefined
    | block_time
    | {block_time, pos_integer()}.

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

-spec parse_timestamp(binary() | undefined) -> {ok, calendar:datetime()} | {error, term()}.
parse_timestamp(Timestamp) ->
    parse_timestamp(trunc_timestamp(calendar:universal_time()), Timestamp).

%% Parse a given binary timestamp. Returns the given `Now' time if passed
%% <<"now">> or undefined. Otherwise an attempt is made to parse an interval relative
%% to `Now', like "-1 week", or "-30 days". If an interval is parsed `Now' +
%% the interval is returned. Failing all, a standard iso8601 parsing attempt is
%% made.
-spec parse_timestamp(Now :: calendar:datetime(), binary() | undefined) ->
    {ok, calendar:datetime()} | {error, term()}.
parse_timestamp(Now, undefined) ->
    {ok, Now};
parse_timestamp(Now, <<"now">>) ->
    parse_timestamp(Now, undefined);
parse_timestamp(Now, Bin) ->
    case parse_interval(Bin) of
        {ok, {_, Interval}} ->
            {ok,
                trunc_timestamp(
                    calendar:gregorian_seconds_to_datetime(
                        calendar:datetime_to_gregorian_seconds(Now) +
                            interval_to_seconds(Interval)
                    )
                )};
        {error, _} ->
            try
                {ok, trunc_timestamp(iso8601:parse(Bin))}
            catch
                error:badarg ->
                    {error, badarg}
            end
    end.

trunc_timestamp({Date, {H, M, _S}}) ->
    {Date, {H, M, 0}}.

-spec parse_timespan(High :: binary() | undefined, Low :: binary() | undefined) ->
    {ok, timespan()} | {error, term()}.
parse_timespan(MaxTime0, MinTime0) ->
    Validate = fun(Max, Min) ->
        calendar:datetime_to_gregorian_seconds(Max) >=
            calendar:datetime_to_gregorian_seconds(Min)
    end,
    %% Parse max_time relative to the current time
    case parse_timestamp(trunc_timestamp(calendar:universal_time()), MaxTime0) of
        {ok, MaxTime} ->
            %% Then parse min_time relative to max_time
            case parse_timestamp(MaxTime, MinTime0) of
                {ok, MinTime} ->
                    %% Validate min/max ordering
                    case Validate(MaxTime, MinTime) of
                        true -> {ok, {MaxTime, MinTime}};
                        false -> {error, badarg}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

-spec parse_bucket(integer(), binary()) -> {ok, interval_spec()} | {error, term()}.
parse_bucket(N, Bin) ->
    parse_interval(N, Bin).

-spec parse_interval(binary()) -> {ok, interval_spec()} | {error, term()}.
parse_interval(Bin) ->
    TrimmedBin = string:trim(Bin),
    try
        case string:split(TrimmedBin, " ", leading) of
            [NumBin, Bucket] ->
                Num = binary_to_integer(NumBin),
                parse_interval(Num, string:trim(Bucket));
            _ ->
                {error, badarg}
        end
    catch
        error:badarg ->
            {error, badarg}
    end.

parse_interval(N, <<"week">>) ->
    {ok, {<<"week">>, {{0, 0, 0}, N * 7, 0}}};
parse_interval(N, <<"day">>) ->
    {ok, {<<"day">>, {{0, 0, 0}, N, 0}}};
parse_interval(N, <<"hour">>) ->
    {ok, {<<"hour">>, {{N, 0, 0}, 0, 0}}};
parse_interval(_, _) ->
    {error, badarg}.

-define(HOURS_TO_SECS(H), (60 * 60 * H)).
-define(MINUTES_TO_SECS(M), (60 * (M))).

-spec interval_to_seconds(epgsql:pg_interval()) -> non_neg_integer().
interval_to_seconds({{H, M, S}, D, 0}) ->
    TimeSecs = S + ?MINUTES_TO_SECS(M) + ?HOURS_TO_SECS(H),
    DaySecs = D * ?HOURS_TO_SECS(24),
    TimeSecs + DaySecs.

-spec parse_bucketed_timespan(High :: binary(), Low :: binary(), Step :: binary()) ->
    {ok, {timespan(), interval_spec()}}
    | {error, term()}.
parse_bucketed_timespan(MaxTime0, MinTime0, Bucket0) ->
    case parse_timespan(MaxTime0, MinTime0) of
        {ok, {MaxTime, MinTime}} ->
            case parse_bucket(-1, Bucket0) of
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
    {ok, Json :: (map() | list()), Cursor :: map() | undefined, Meta :: map() | undefined}
    | {ok, Json :: (map() | list()), Cursor :: map() | undefined}
    | {ok, Json :: (map() | list())}
    | {error, term()},
    cache_time()
) -> {ok | elli:response_code(), elli:headers(), elli:body()}.
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
    %% Set to 10 days (86400 seconds in 24 hours)
    [
        {<<"Surrogate-Control">>, <<"max-age=864000">>},
        {<<"Cache-Control">>, <<"max-age=864000">>}
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

insert_location_hex(Location, Fields) ->
    insert_location_hex(Location, <<"location_hex">>, Fields).

insert_location_hex(Location, Name, Fields) ->
    Fields#{Name => to_location_hex(Location)}.

-spec to_location_hex(binary()) -> binary().
to_location_hex(Location) ->
    {Lat, Lon} = h3:to_geo(h3:from_string(binary_to_list(Location))),
    list_to_binary(h3:to_string(h3:from_geo({Lat, Lon}, ?LOCATION_HEX_RES))).

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

-spec parse_float(null | binary() | float()) -> float().
parse_float(null) ->
    null;
parse_float(Bin) when is_binary(Bin) ->
    binary_to_float(Bin);
parse_float(Num) when is_float(Num) ->
    Num.

parse_int(null) ->
    null;
parse_int(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin);
parse_int(Num) when is_integer(Num) ->
    Num.

try_or_else(TryFun, Fun, OrElse) ->
    try TryFun() of
        V -> Fun(V)
    catch
        _:_ ->
            OrElse
    end.

filter_types_to_list(Base, undefined) ->
    Base;
filter_types_to_list(Base, Bin) when is_binary(Bin) ->
    SplitTypes = binary:split(Bin, <<",">>, [global]),
    lists:filter(fun(T) -> lists:member(T, Base) end, SplitTypes).

-spec filter_types_to_sql(list(), undefined | [binary()] | binary()) -> iolist().
filter_types_to_sql(Base, undefined) ->
    filter_types_to_sql(Base, Base);
filter_types_to_sql(Base, Bin) when is_binary(Bin) ->
    filter_types_to_sql(Base, filter_types_to_list(Base, Bin));
filter_types_to_sql(_Base, Types) when is_list(Types) ->
    [<<"{">>, lists:join(<<",">>, Types), <<"}">>].

hotspot_modes_to_list(Base, undefined) ->
    Base;
hotspot_modes_to_list(Base, Bin) when is_binary(Bin) ->
    SplitTypes = binary:split(Bin, <<",">>, [global]),
    hotspot_modes_to_list(Base, SplitTypes);
hotspot_modes_to_list(Base, List) when is_list(List) ->
    lists:filter(fun(T) -> lists:member(T, Base) end, List).

-spec hotspot_modes_to_sql(list(), undefined | [binary()] | binary()) -> iolist().
hotspot_modes_to_sql(Base, undefined) ->
    filter_types_to_sql(Base, Base);
hotspot_modes_to_sql(Base, Bin) when is_binary(Bin) ->
    filter_types_to_sql(Base, filter_types_to_list(Base, Bin));
hotspot_modes_to_sql(_Base, Types) when is_list(Types) ->
    [<<"{">>, lists:join(<<",">>, Types), <<"}">>].

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

parse_timestamp_test() ->
    Now = {{2021, 1, 28}, {0, 0, 0}} = iso8601:parse("2021-01-28"),
    %% Basic iso8601 parsing
    ?assertEqual({ok, Now}, parse_timestamp(Now, "2021-01-28")),
    ?assertEqual(
        {ok, {{2021, 1, 29}, {0, 36, 59}}},
        parse_timestamp(Now, "2021-01-29T00:36:59Z")
    ),
    %% invalid timestampr
    ?assertEqual({error, badarg}, parse_timestamp(Now, "no way")),

    %% now/undefined
    ?assertMatch({ok, _}, parse_timestamp(Now, <<"now">>)),
    ?assertMatch({ok, _}, parse_timestamp(Now, undefined)),

    %% intervals
    ?assertMatch({ok, {{2021, 1, 27}, {0, 0, 0}}}, parse_timestamp(Now, <<"-1 day">>)),
    ?assertMatch({ok, {{2021, 1, 21}, {0, 0, 0}}}, parse_timestamp(Now, <<"-1 week">>)),
    ?assertMatch({ok, {{2021, 1, 26}, {0, 0, 0}}}, parse_timestamp(Now, <<"-48 hour">>)),
    ok.

parse_float_test() ->
    ?assertEqual(null, parse_float(null)),
    ?assertEqual(22.2, parse_float(<<"22.2">>)),
    ?assertEqual(22.2, parse_float(22.2)),
    ok.

parse_int_test() ->
    ?assertEqual(null, parse_int(null)),
    ?assertEqual(22, parse_int(<<"22">>)),
    ?assertEqual(22, parse_int(22)),
    ok.

-endif.
