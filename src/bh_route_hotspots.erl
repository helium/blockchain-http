-module(bh_route_hotspots).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([
    get_hotspot_list/1,
    get_hotspot/1,
    to_geo_json/1
]).

-define(S_HOTSPOT_LIST_BEFORE, "hotspot_list_before").
-define(S_HOTSPOT_LIST, "hotspot_list").
-define(S_HOTSPOT_ELECTED_LIST, "hotspot_elected_list").
-define(S_HOTSPOT_ELECTION_LIST, "hotspot_list_election_list").
-define(S_OWNER_HOTSPOT_LIST_BEFORE, "owner_hotspot_list_before").
-define(S_OWNER_HOTSPOT_LIST, "owner_hotspot_list").
-define(S_HOTSPOT, "hotspot").
-define(S_HOTSPOTS_NAMED, "hotspots_named").
-define(S_HOTSPOT_NAME_SEARCH, "hotspots_name_search").
-define(S_HOTSPOT_LOCATION_DISTANCE_SEARCH, "hotspots_location_distance_search").
-define(S_HOTSPOT_LOCATION_DISTANCE_SEARCH_BEFORE, "hotspots_location_distance_search_before").
-define(S_HOTSPOT_LOCATION_BOX_SEARCH, "hotspots_location_box_search").
-define(S_HOTSPOT_LOCATION_BOX_SEARCH_BEFORE, "hotspots_location_box_search_before").
-define(S_CITY_HOTSPOT_LIST, "hotspot_city_list").
-define(S_CITY_HOTSPOT_LIST_BEFORE, "hotspot_city_list_before").
-define(S_HEX_HOTSPOT_LIST, "hotspot_hex_list").
-define(S_HEX_HOTSPOT_LIST_BEFORE, "hotspot_hexlist_before").
-define(S_HOTSPOT_WITNESS_LIST, "hotspot_witness_list").
-define(S_HOTSPOT_BUCKETED_SUM_WITNESSES, "hotspot_bucketed_sum_witnesses").
-define(S_HOTSPOT_BUCKETED_SUM_CHALLENGES, "hotspot_bucketed_sum_challenges").
-define(HOTSPOT_LIST_LIMIT, 1000).
-define(HOTSPOT_LIST_NAME_SEARCH_LIMIT, 100).
-define(HOTSPOT_LIST_LOCATION_SEARCH_LIMIT, 500).

prepare_conn(Conn) ->
    HotspotListLimit = "limit " ++ integer_to_list(?HOTSPOT_LIST_LIMIT),
    HotspotListNameSearchLimit = "limit " ++ integer_to_list(?HOTSPOT_LIST_NAME_SEARCH_LIMIT),
    HotspotListLocationSearchLimit =
        "limit " ++ integer_to_list(?HOTSPOT_LIST_LOCATION_SEARCH_LIMIT),
    Loads = [
        {?S_HOTSPOT_LIST_BEFORE,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, hotspot_list_before_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_HOTSPOT_LIST,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, ""},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_OWNER_HOTSPOT_LIST_BEFORE,
            {hotspot_list_base, [
                {source, owner_hotspot_list_source},
                {scope, owner_hotspot_list_before_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_OWNER_HOTSPOT_LIST,
            {hotspot_list_base, [
                {source, owner_hotspot_list_source},
                {scope, ""},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_HOTSPOT,
            {hotspot_list_base, [
                {source, hotspot_source},
                {scope, "where g.address = $1"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_HOTSPOTS_NAMED,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, "where g.name = $1"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_HOTSPOT_NAME_SEARCH,
            {hotspot_list_base, [
                {source, hotspot_name_search_source},
                {scope, hotspot_name_search_scope},
                {order, hotspot_name_search_order},
                {limit, HotspotListNameSearchLimit}
            ]}},
        {?S_HOTSPOT_LOCATION_BOX_SEARCH,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, hotspot_location_box_search_scope},
                {order, hotspot_location_box_search_order},
                {limit, HotspotListLocationSearchLimit}
            ]}},
        {?S_HOTSPOT_LOCATION_BOX_SEARCH_BEFORE,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, hotspot_location_box_search_before_scope},
                {order, hotspot_location_box_search_order},
                {limit, HotspotListLocationSearchLimit}
            ]}},
        {?S_HOTSPOT_LOCATION_DISTANCE_SEARCH,
            {hotspot_list_base, [
                {source, hotspot_location_distance_search_source},
                {scope, hotspot_location_distance_search_scope},
                {order, hotspot_location_distance_search_order},
                {limit, HotspotListLocationSearchLimit}
            ]}},
        {?S_HOTSPOT_LOCATION_DISTANCE_SEARCH_BEFORE,
            {hotspot_list_base, [
                {source, hotspot_location_distance_search_source},
                {scope, hotspot_location_distance_search_before_scope},
                {order, hotspot_location_distance_search_order},
                {limit, HotspotListLocationSearchLimit}
            ]}},
        {?S_CITY_HOTSPOT_LIST_BEFORE,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, city_hotspot_list_before_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_CITY_HOTSPOT_LIST,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, city_hotspot_list_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_HEX_HOTSPOT_LIST_BEFORE,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, hex_hotspot_list_before_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_HEX_HOTSPOT_LIST,
            {hotspot_list_base, [
                {source, hotspot_list_source},
                {scope, hex_hotspot_list_scope},
                {order, hotspot_list_order},
                {limit, HotspotListLimit}
            ]}},
        {?S_HOTSPOT_WITNESS_LIST,
            {hotspot_witness_list, [
                {hotspot_select,
                    {hotspot_list_base, [
                        {source, hotspot_witness_list_source},
                        {scope, ""},
                        {order, hotspot_list_order},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_HOTSPOT_ELECTED_LIST,
            {hotspot_elected_list, [
                {filter, "and block <= $1"},
                {hotspot_select,
                    {hotspot_list_base, [
                        {source, hotspot_list_source},
                        {scope, hotspot_elected_list_scope},
                        {order, ""},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_HOTSPOT_ELECTION_LIST,
            {hotspot_elected_list, [
                {filter, "and hash = $1"},
                {hotspot_select,
                    {hotspot_list_base, [
                        {source, hotspot_list_source},
                        {scope, hotspot_elected_list_scope},
                        {order, ""},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_HOTSPOT_BUCKETED_SUM_WITNESSES,
            {hotspot_bucketed_witnesses_base, [
                {scope, "where g.address = $1"},
                {source, hotspot_bucketed_witnesses_source}
            ]}},
        {?S_HOTSPOT_BUCKETED_SUM_CHALLENGES,
            {hotspot_bucketed_challenges_base, [
                {scope, "where a.actor = $1"},
                {source, hotspot_bucketed_challenges_source}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "hotspots.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(
        get_hotspot_list([{owner, undefined}, {city, undefined} | Args]),
        block_time
    );
handle('GET', [<<"elected">>], _Req) ->
    {ok, #{height := Height}} = bh_route_blocks:get_block_height(),
    ?MK_RESPONSE(get_hotspot_elected_list({height, Height}), block_time);
handle('GET', [<<"elected">>, BlockId], _Req) ->
    bh_route_handler:try_or_else(
        fun() -> binary_to_integer(BlockId) end,
        fun(Height) ->
            ?MK_RESPONSE(get_hotspot_elected_list({height, Height}), infinity)
        end,
        ?RESPONSE_400
    );
handle('GET', [<<"elected">>, <<"hash">>, TxnHash], _Req) ->
    ?MK_RESPONSE(get_hotspot_elected_list({hash, TxnHash}), infinity);
handle('GET', [<<"name">>], Req) ->
    Args = ?GET_ARGS([search], Req),
    ?MK_RESPONSE(get_hotspot_list(Args), block_time);
handle('GET', [<<"name">>, Name], _Req) ->
    ?MK_RESPONSE(get_hotspots_named(Name), block_time);
handle('GET', [<<"location">>, <<"box">>], Req) ->
    Args = ?GET_ARGS([swlon, swlat, nelon, nelat, cursor], Req),
    ?MK_RESPONSE(get_hotspot_list_in_box(Args), block_time);
handle('GET', [<<"location">>, <<"distance">>], Req) ->
    Args = ?GET_ARGS([lat, lon, distance, cursor], Req),
    ?MK_RESPONSE(get_hotspot_list_by_distance(Args), block_time);
handle('GET', [<<"hex">>, Location], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_hotspot_list([{location_hex, Location}] ++ Args), block_time);
handle('GET', [Address, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    Result = bh_route_txns:get_activity_list({hotspot, Address}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Address, <<"activity">>, <<"count">>], Req) ->
    Args = ?GET_ARGS([filter_types], Req),
    ?MK_RESPONSE(bh_route_txns:get_activity_count({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"elections">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(
        bh_route_elections:get_election_list({hotspot, Address}, Args),
        block_time
    );
handle('GET', [Address, <<"challenges">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(
        bh_route_challenges:get_challenge_list({hotspot, Address}, Args),
        block_time
    );
handle('GET', [Address, <<"challenges">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(get_challenges_sum({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_list({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_sum({hotspot, Address}, Args), block_time);
handle('GET', [<<"rewards">>, <<"sum">>], Req) ->
    %% We do not allow bucketing across all hotspots as that takes way too long
    Args = ?GET_ARGS([max_time, min_time], Req),
    ?MK_RESPONSE(
        bh_route_rewards:get_reward_sum({hotspot, all}, Args ++ [{bucket, undefined}]),
        block_time
    );
handle('GET', [Address, <<"witnesses">>], _Req) ->
    ?MK_RESPONSE(get_hotspot_list([{witnesses_for, Address}]), block_time);
handle('GET', [Address, <<"witnesses">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(get_witnesses_sum({hotspot, Address}, Args), block_time);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address), block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_hotspot_elected_list({height, Height}) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_ELECTED_LIST, [Height]),
    mk_hotspot_list_from_result(Result);
get_hotspot_elected_list({hash, TxnHash}) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_ELECTION_LIST, [TxnHash]),
    mk_hotspot_list_from_result(Result).

get_hotspot_list_in_box([
    {swlon, SWLon},
    {swlat, SWLat},
    {nelon, NELon},
    {nelat, NELat},
    {cursor, undefined}
]) ->
    Args =
        try
            [?PARSE_FLOAT(SWLon), ?PARSE_FLOAT(SWLat), ?PARSE_FLOAT(NELon), ?PARSE_FLOAT(NELat)]
        catch
            _:_ ->
                throw(?RESPONSE_400)
        end,
    get_hotspot_list([{search_box, Args}, {cursor, undefined}]);
get_hotspot_list_in_box([
    {swlon, _},
    {swlat, _},
    {nelon, _},
    {nelat, _},
    {cursor, Cursor}
]) ->
    get_hotspot_list([{search_box, []}, {cursor, Cursor}]).

get_hotspot_list_by_distance([{lat, Lat}, {lon, Lon}, {distance, Distance}, {cursor, undefined}]) ->
    Args =
        try
            [?PARSE_FLOAT(Lat), ?PARSE_FLOAT(Lon), ?PARSE_INT(Distance)]
        catch
            _:_ ->
                throw(?RESPONSE_400)
        end,
    get_hotspot_list([{search_distance, Args}, {cursor, undefined}]);
get_hotspot_list_by_distance([{lat, _}, {lon, _}, {distance, _}, {cursor, Cursor}]) ->
    get_hotspot_list([{search_distance, []}, {cursor, Cursor}]).

get_hotspot_list([{witnesses_for, Address}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_WITNESS_LIST, [Address]),
    mk_hotspot_witness_list_from_result(Result);
get_hotspot_list([{owner, undefined}, {city, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST, []),
    mk_hotspot_list_from_result(Result);
get_hotspot_list([{owner, Owner}, {city, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST, [Owner]),
    mk_hotspot_list_from_result(Result);
get_hotspot_list([{owner, undefined}, {city, City}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_HOTSPOT_LIST, [City]),
    mk_hotspot_list_from_result(Result);
get_hotspot_list([{owner, Owner}, {city, City}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            case {Owner, City} of
                {undefined, undefined} ->
                    Result =
                        ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(Result);
                {Owner, undefined} ->
                    Result =
                        ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST_BEFORE, [
                            Owner,
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_hotspot_list_from_result(Result);
                {undefined, City} ->
                    Result =
                        ?PREPARED_QUERY(?S_CITY_HOTSPOT_LIST_BEFORE, [
                            City,
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_hotspot_list_from_result(Result);
                {_, _} ->
                    {error, badarg}
            end;
        _ ->
            {error, badarg}
    end;
get_hotspot_list([{search_distance, [Lat, Lon, Distance]}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LOCATION_DISTANCE_SEARCH, [Lon, Lat, Distance]),
    mk_hotspot_list_from_result(
        ?HOTSPOT_LIST_LOCATION_SEARCH_LIMIT,
        #{lat => Lat, lon => Lon, distance => Distance},
        Result
    );
get_hotspot_list([{search_distance, _}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"lat">> := Lat,
            <<"lon">> := Lon,
            <<"distance">> := Distance,
            <<"before_address">> := BeforeAddress,
            <<"before_distance">> := BeforeDistance,
            <<"height">> := _Height
        }} ->
            Result = ?PREPARED_QUERY(
                ?S_HOTSPOT_LOCATION_DISTANCE_SEARCH_BEFORE,
                [Lon, Lat, Distance] ++ [BeforeAddress, BeforeDistance]
            ),
            mk_hotspot_list_from_result(
                ?HOTSPOT_LIST_LOCATION_SEARCH_LIMIT,
                #{lat => Lat, lon => Lon, distance => Distance},
                Result
            );
        _ ->
            {error, badarg}
    end;
get_hotspot_list([{search_box, Corners}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LOCATION_BOX_SEARCH, Corners),
    mk_hotspot_list_from_result(
        ?HOTSPOT_LIST_LOCATION_SEARCH_LIMIT,
        #{corners => Corners},
        Result
    );
get_hotspot_list([{search_box, _}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"corners">> := Corners,
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            Result = ?PREPARED_QUERY(
                ?S_HOTSPOT_LOCATION_BOX_SEARCH_BEFORE,
                Corners ++ [BeforeAddress, BeforeBlock]
            ),
            mk_hotspot_list_from_result(
                ?HOTSPOT_LIST_LOCATION_SEARCH_LIMIT,
                #{corners => Corners},
                Result
            );
        _ ->
            {error, badarg}
    end;
get_hotspot_list([{search, Name}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_NAME_SEARCH, [Name]),
    mk_hotspot_list_from_result(Result);
get_hotspot_list([{location_hex, Location}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HEX_HOTSPOT_LIST, [Location]),
    mk_hotspot_list_from_result(Result);
get_hotspot_list([{location_hex, _}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"location">> := Location,
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            Result =
                ?PREPARED_QUERY(?S_HEX_HOTSPOT_LIST_BEFORE, [Location, BeforeAddress, BeforeBlock]),
            mk_hotspot_list_from_result(Result);
        _ ->
            {error, badarg}
    end.

get_witnesses_sum(
    {hotspot, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, _}]
) ->
    get_witness_bucketed_sum([Address], ?S_HOTSPOT_BUCKETED_SUM_WITNESSES, Args).

get_witness_bucketed_sum(Args, Query, [
    {max_time, MaxTime0},
    {min_time, MinTime0},
    {bucket, Bucket}
]) ->
    case ?PARSE_BUCKETED_TIMESPAN(MaxTime0, MinTime0, Bucket) of
        {ok, {{MaxTime, MinTime}, {BucketType, BucketStep}}} ->
            Result = ?PREPARED_QUERY(Query, Args ++ [MaxTime, MinTime, BucketStep]),
            mk_buckets_result(
                fun witness_bucket_to_json/1,
                MaxTime,
                MinTime,
                BucketType,
                Result
            );
        {error, Error} ->
            {error, Error}
    end.

get_challenges_sum(
    {hotspot, Address},
    Args = [{max_time, _}, {min_time, _}, {bucket, _}]
) ->
    get_challenges_bucketed_sum([Address], ?S_HOTSPOT_BUCKETED_SUM_CHALLENGES, Args).

get_challenges_bucketed_sum(Args, Query, [
    {max_time, MaxTime0},
    {min_time, MinTime0},
    {bucket, Bucket}
]) ->
    case ?PARSE_BUCKETED_TIMESPAN(MaxTime0, MinTime0, Bucket) of
        {ok, {{MaxTime, MinTime}, {BucketType, BucketStep}}} ->
            Result = ?PREPARED_QUERY(Query, Args ++ [MaxTime, MinTime, BucketStep]),
            mk_buckets_result(
                fun challenge_bucket_to_json/1,
                MaxTime,
                MinTime,
                BucketType,
                Result
            );
        {error, Error} ->
            {error, Error}
    end.

get_hotspot(Address) ->
    case ?PREPARED_QUERY(?S_HOTSPOT, [Address]) of
        {ok, _, [Result]} ->
            {ok, hotspot_to_json(Result)};
        _ ->
            {error, not_found}
    end.

get_hotspots_named(Name) ->
    case ?PREPARED_QUERY(?S_HOTSPOTS_NAMED, [Name]) of
        {ok, _, Results} ->
            {ok, hotspot_list_to_json(Results)};
        _ ->
            {error, not_found}
    end.

mk_hotspot_list_from_result(Results) ->
    mk_hotspot_list_from_result(?HOTSPOT_LIST_LIMIT, #{}, Results).

mk_hotspot_list_from_result(Limit, CursorBase, {ok, _, Results}) ->
    {ok, hotspot_list_to_json(Results), mk_cursor(Limit, CursorBase, Results)}.

mk_hotspot_witness_list_from_result({ok, _, Results}) ->
    {ok, hotspot_witness_list_to_json(Results)}.

mk_cursor(Limit, CursorBase, Results) when is_list(Results) ->
    case length(Results) < Limit of
        true ->
            undefined;
        false ->
            case lists:last(Results) of
                {Height, _LastChangeBlock, FirstBlock, _FirstTimestamp, _LastPocChallenge, Address,
                    _Mode, _Owner, _Location, _Nonce, _Name, _RewardScale, _Elevation, _Gain,
                    _OnlineStatus, _BlockStatus, _ListenAddrs, _ShortStreet, _LongStreet,
                    _ShortCity, _LongCity, _ShortState, _LongState, _ShortCountry, _LongCountry,
                    _CityId} ->
                    CursorBase#{
                        before_address => Address,
                        before_block => FirstBlock,
                        %% Add height to the cursor to avoid overlap between the
                        %% same address/block and a page of hotspot data at a
                        %% different height
                        height => Height
                    };
                {Height, _LastChangeBlock, _FirstBlock, _FirstTimestamp, _LastPocChallenge, Address,
                    _Mode, _Owner, _Location, _Nonce, _Name, _RewardScale, _Elevation, _Gain,
                    _OnlineStatus, _BlockStatus, _ListenAddrs, _ShortStreet, _LongStreet,
                    _ShortCity, _LongCity, _ShortState, _LongState, _ShortCountry, _LongCountry,
                    _CityId, Distance} ->
                    CursorBase#{
                        before_address => Address,
                        before_distance => Distance,
                        %% Add height to the cursor to avoid overlap between the
                        %% same address/block and a page of hotspot data at a
                        %% different height
                        height => Height
                    }
            end
    end.

mk_buckets_result(Fun, MaxTime, MinTime, BucketType, {ok, _, Results}) ->
    Meta = #{
        max_time => iso8601:format(MaxTime),
        min_time => iso8601:format(MinTime),
        bucket => BucketType
    },
    {ok, buckets_to_json(Fun, Results), undefined, Meta}.

%%
%% to_jaon
%%

hotspot_list_to_json(Results) ->
    lists:map(fun hotspot_to_json/1, Results).

hotspot_witness_list_to_json(Results) ->
    lists:map(fun hotspot_witness_to_json/1, Results).

to_geo_json(
    {ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry,
        CityId}
) ->
    Base = to_geo_json(
        {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId}
    ),
    Base#{
        short_street => ShortStreet,
        long_street => LongStreet
    };
to_geo_json(
    {ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId}
) ->
    MaybeB64 = fun
        (null) -> null;
        (Bin) -> ?BIN_TO_B64(Bin)
    end,
    #{
        short_city => ShortCity,
        long_city => LongCity,
        short_state => ShortState,
        long_state => LongState,
        short_country => ShortCountry,
        long_country => LongCountry,
        city_id => MaybeB64(CityId)
    }.

hotspot_witness_to_json(
    {Height, LastChangeBlock, FirstBlock, FirstTimestamp, LastPoCChallenge, Address, Mode, Owner,
        Location, Nonce, Name, RewardScale, Elevation, Gain, OnlineStatus, BlockStatus, ListenAddrs,
        ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry,
        LongCountry, CityId, WitnessFor, WitnessInfo}
) ->
    Base = hotspot_to_json(
        {Height, LastChangeBlock, FirstBlock, FirstTimestamp, LastPoCChallenge, Address, Mode,
            Owner, Location, Nonce, Name, RewardScale, Elevation, Gain, OnlineStatus, BlockStatus,
            ListenAddrs, ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState,
            ShortCountry, LongCountry, CityId}
    ),
    Base#{
        witness_for => WitnessFor,
        witness_info => WitnessInfo
    }.

hotspot_to_json(
    {Height, LastChangeBlock, FirstBlock, FirstTimestamp, LastPoCChallenge, Address, Mode, Owner,
        Location, Nonce, Name, RewardScale, Elevation, Gain, OnlineStatus, BlockStatus, ListenAddrs,
        ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry,
        LongCountry, CityId, SpecNonce}
) ->
    Base = hotspot_to_json(
        {Height, LastChangeBlock, FirstBlock, FirstTimestamp, LastPoCChallenge, Address, Mode,
            Owner, Location, Nonce, Name, RewardScale, Elevation, Gain, OnlineStatus, BlockStatus,
            ListenAddrs, ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState,
            ShortCountry, LongCountry, CityId}
    ),
    Base#{
        <<"speculative_nonce">> => SpecNonce
    };
hotspot_to_json(
    {Height, LastChangeBlock, FirstBlock, FirstTimestamp, LastPoCChallenge, Address, Mode, Owner,
        Location, Nonce, Name, RewardScale, Elevation, Gain, OnlineStatus, BlockStatus, ListenAddrs,
        ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry,
        LongCountry, CityId}
) ->
    MaybeZero = fun
        (null) -> 0;
        (V) -> V
    end,
    ?INSERT_LAT_LON(
        Location,
        #{
            address => Address,
            mode => Mode,
            name => Name,
            owner => Owner,
            location => Location,
            elevation => Elevation,
            gain => Gain,
            geocode => to_geo_json(
                {ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry,
                    LongCountry, CityId}
            ),
            last_change_block => LastChangeBlock,
            last_poc_challenge => LastPoCChallenge,
            reward_scale => RewardScale,
            block_added => FirstBlock,
            timestamp_added => iso8601:format(FirstTimestamp),
            block => Height,
            status => #{
                online => OnlineStatus,
                height => BlockStatus,
                listen_addrs => ListenAddrs
            },
            nonce => MaybeZero(Nonce)
        }
    ).

buckets_to_json(Fun, Results) ->
    lists:map(Fun, Results).

witness_bucket_to_json({Min, Max, Median, Avg, StdDev}) ->
    #{
        min => Min,
        max => Max,
        median => Median,
        avg => Avg,
        stddev => StdDev
    };
witness_bucket_to_json({Timestamp, Min, Max, Median, Avg, StdDev}) ->
    Base = witness_bucket_to_json({Min, Max, Median, Avg, StdDev}),
    Base#{
        timestamp => iso8601:format(Timestamp)
    }.

challenge_bucket_to_json({Min, Max, Sum, Median, Avg, StdDev}) ->
    #{
        min => Min,
        max => Max,
        sum => Sum,
        median => Median,
        avg => Avg,
        stddev => StdDev
    };
challenge_bucket_to_json({Timestamp, Min, Max, Sum, Median, Avg, StdDev}) ->
    Base = challenge_bucket_to_json({Min, Max, Sum, Median, Avg, StdDev}),
    Base#{
        timestamp => iso8601:format(Timestamp)
    }.
