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
-define(S_HOTSPOT_LIST_ELECTED, "hotspot_list_elected").
-define(S_OWNER_HOTSPOT_LIST_BEFORE, "owner_hotspot_list_before").
-define(S_OWNER_HOTSPOT_LIST, "owner_hotspot_list").
-define(S_HOTSPOT, "hotspot").
-define(S_HOTSPOTS_NAMED, "hotspots_named").
-define(S_CITY_HOTSPOT_LIST, "hotspot_city_list").
-define(S_CITY_HOTSPOT_LIST_BEFORE, "hotspot_city_list_before").
-define(S_HOTSPOT_WITNESS_LIST, "hotspot_witness_list").

-define(SELECT_HOTSPOT_BASE(G), [
    "select (select max(height) from blocks) as height, ",
    "g.last_block, g.first_block, g.address, g.owner, g.location, g.score, g.nonce, g.name, ",
    "s.online as online_status, s.block as block_status, "
    "l.short_street, l.long_street, ",
    "l.short_city, l.long_city, ",
    "l.short_state, l.long_state, ",
    "l.short_country, l.long_country, ",
    "l.city_id ",
    G,
    " left join locations l on g.location = l.location ",
    " left join gateway_status s on s.address = g.address "
]).

-define(SELECT_HOTSPOT_BASE, ?SELECT_HOTSPOT_BASE("from gateway_inventory g")).
-define(SELECT_OWNER_HOTSPOT,
    ?SELECT_HOTSPOT_BASE(["from (select * from gateway_inventory where owner = $1) as g"])
).

-define(HOTSPOT_LIST_LIMIT, 1000).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(
        Conn,
        ?S_HOTSPOT_LIST_BEFORE,
        [
            ?SELECT_HOTSPOT_BASE,
            "where ((g.address > $1 and g.first_block = $2) or (g.first_block < $2)) ",
            "order by g.first_block desc, g.address ",
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S2} = epgsql:parse(
        Conn,
        ?S_HOTSPOT_LIST,
        [
            ?SELECT_HOTSPOT_BASE,
            "order by g.first_block desc, g.address ",
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S3} = epgsql:parse(
        Conn,
        ?S_OWNER_HOTSPOT_LIST_BEFORE,
        [
            ?SELECT_OWNER_HOTSPOT,
            "where ((g.address > $2 and g.first_block = $3) or (g.first_block < $3)) ",
            "order by g.first_block desc, g.address "
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S4} = epgsql:parse(
        Conn,
        ?S_OWNER_HOTSPOT_LIST,
        [
            ?SELECT_OWNER_HOTSPOT,
            "order by g.first_block desc, g.address ",
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S5} = epgsql:parse(
        Conn,
        ?S_HOTSPOT,
        [
            ?SELECT_HOTSPOT_BASE,
            "where g.address = $1"
        ],
        []
    ),

    {ok, S6} = epgsql:parse(
        Conn,
        ?S_HOTSPOTS_NAMED,
        [
            ?SELECT_HOTSPOT_BASE,
            "where g.name = $1"
        ],
        []
    ),

    {ok, S7} = epgsql:parse(
        Conn,
        ?S_CITY_HOTSPOT_LIST_BEFORE,
        [
            ?SELECT_HOTSPOT_BASE,
            "where l.city_id = $1 "
            "and ((g.address > $2 and g.first_block = $3) or (g.first_block < $3)) ",
            "order by g.first_block desc, g.address "
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S8} = epgsql:parse(
        Conn,
        ?S_CITY_HOTSPOT_LIST,
        [
            ?SELECT_HOTSPOT_BASE,
            "where l.city_id = $1 "
            "order by g.first_block desc, g.address ",
            "limit ",
            integer_to_list(?HOTSPOT_LIST_LIMIT)
        ],
        []
    ),

    {ok, S9} = epgsql:parse(
        Conn,
        ?S_HOTSPOT_WITNESS_LIST,
        [
            "with hotspot_witnesses as ( ",
            "    select gi.address as witness_for, w.key as witness, w.value as witness_info ",
            "    from gateway_inventory gi, jsonb_each(gi.witnesses) w where gi.address = $1 ",
            ")",
            ?SELECT_HOTSPOT_BASE([
                ", g.witness_for, g.witness_info ",
                "from (select * from hotspot_witnesses w inner join gateway_inventory i on (w.witness = i.address)) g"
            ]),
            "order by g.first_block, g.address"
        ],
        []
    ),

    {ok, S10} = epgsql:parse(
        Conn,
        ?S_HOTSPOT_LIST_ELECTED,
        [
            "with field_members as ( ",
            "    select fields->'members' as members ",
            "    from transactions ",
            "    where type = 'consensus_group_v1' order by block desc limit 1 ",
            "),",
            "members as ( ",
            "    select * from jsonb_array_elements_text((select members from field_members)) ",
            ") ",
            ?SELECT_HOTSPOT_BASE,
            "where g.address in (select * from members)"
        ],
        []
    ),

    #{
        ?S_HOTSPOT_LIST_BEFORE => S1,
        ?S_HOTSPOT_LIST => S2,
        ?S_OWNER_HOTSPOT_LIST_BEFORE => S3,
        ?S_OWNER_HOTSPOT_LIST => S4,
        ?S_HOTSPOT => S5,
        ?S_HOTSPOTS_NAMED => S6,
        ?S_CITY_HOTSPOT_LIST_BEFORE => S7,
        ?S_CITY_HOTSPOT_LIST => S8,
        ?S_HOTSPOT_WITNESS_LIST => S9,
        ?S_HOTSPOT_LIST_ELECTED => S10
    }.

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_hotspot_list([{owner, undefined}, {city, undefined} | Args]), block_time);
handle('GET', [<<"elected">>], __Req) ->
    ?MK_RESPONSE(get_hotspot_elected_list(), block_time);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address), block_time);
handle('GET', [<<"name">>, Name], _Req) ->
    ?MK_RESPONSE(get_hotspots_named(Name), block_time);
handle('GET', [Address, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    Result = bh_route_txns:get_activity_list({hotspot, Address}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Address, <<"elections">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_elections:get_election_list({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"challenges">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_challenges:get_challenge_list({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_list({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_sum({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>, <<"stats">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_stats({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"witnesses">>], _Req) ->
    ?MK_RESPONSE(get_hotspot_list([{witnesses_for, Address}]), block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_hotspot_elected_list() ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_ELECTED, []),
    mk_hotspot_list_from_result(Result).

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
                    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(Result);
                {Owner, undefined} ->
                    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST_BEFORE, [
                        Owner,
                        BeforeAddress,
                        BeforeBlock
                    ]),
                    mk_hotspot_list_from_result(Result);
                {undefined, City} ->
                    Result = ?PREPARED_QUERY(?S_CITY_HOTSPOT_LIST_BEFORE, [
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

mk_hotspot_list_from_result({ok, _, Results}) ->
    {ok, hotspot_list_to_json(Results), mk_cursor(Results)}.

mk_hotspot_witness_list_from_result({ok, _, Results}) ->
    {ok, hotspot_witness_list_to_json(Results)}.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?HOTSPOT_LIST_LIMIT of
        true ->
            undefined;
        false ->
            case lists:last(Results) of
                {Height, _ScoreBlock, FirstBlock, Address, _Owner, _Location, _Score, _Nonce, _Name,
                    _OnlineStatus, _BlockStatus, _ShortStreet, _LongStreet, _ShortCity, _LongCity,
                    _ShortState, _LongState, _ShortCountry, _LongCountry, _CityId} ->
                    #{
                        before_address => Address,
                        before_block => FirstBlock,
                        %% Add height to the cursor to avoid overlap between the
                        %% same address/block and a page of hotspot data at a
                        %% different height
                        height => Height
                    }
            end
    end.

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
to_geo_json({ShortCity, LongCity, ShortState, LongState, ShortCountry, LongCountry, CityId}) ->
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
    {Height, ScoreBlock, FirstBlock, Address, Owner, Location, Score, Nonce, Name, OnlineStatus,
        BlockStatus, ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState,
        ShortCountry, LongCountry, CityId, WitnessFor, WitnessInfo}
) ->
    Base = hotspot_to_json(
        {Height, ScoreBlock, FirstBlock, Address, Owner, Location, Score, Nonce, Name, OnlineStatus,
            BlockStatus, ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState,
            ShortCountry, LongCountry, CityId}
    ),
    Base#{
        witness_for => WitnessFor,
        witness_info => WitnessInfo
    }.

hotspot_to_json(
    {Height, ScoreBlock, FirstBlock, Address, Owner, Location, Score, Nonce, Name, OnlineStatus,
        BlockStatus, ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState,
        ShortCountry, LongCountry, CityId}
) ->
    MaybeZero = fun
        (null) -> 0;
        (V) -> V
    end,
    ?INSERT_LAT_LON(
        Location,
        #{
            address => Address,
            name => Name,
            owner => Owner,
            location => Location,
            geocode => to_geo_json(
                {ShortStreet, LongStreet, ShortCity, LongCity, ShortState, LongState, ShortCountry,
                    LongCountry, CityId}
            ),
            score_update_height => ScoreBlock,
            score => Score,
            block_added => FirstBlock,
            block => Height,
            status => #{
                online => OnlineStatus,
                height => BlockStatus
            },
            nonce => MaybeZero(Nonce)
        }
    ).
