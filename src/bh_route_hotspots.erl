-module(bh_route_hotspots).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_hotspot_list/1,
         get_hotspot/1]).


-define(S_HOTSPOT_LIST_BEFORE, "hotspot_list_before").
-define(S_HOTSPOT_LIST, "hotspot_list").
-define(S_OWNER_HOTSPOT_LIST_BEFORE, "owner_hotspot_list_before").
-define(S_OWNER_HOTSPOT_LIST, "owner_hotspot_list").
-define(S_HOTSPOT, "hotspot").

-define(SELECT_HOTSPOT_BASE(G),
        ["select (select max(height) from blocks) as height, g.last_block, g.first_block, g.address, g.owner, g.location, g.score, ",
         "l.short_street, l.long_street, l.short_city, l.long_city, l.short_state, l.long_state, l.short_country, l.long_country ",
         G, " inner join locations l on g.location = l.location "
        ]).
-define(SELECT_HOTSPOT_BASE, ?SELECT_HOTSPOT_BASE("from gateway_inventory g")).
-define(SELECT_OWNER_HOTSPOT, ?SELECT_HOTSPOT_BASE("from (select * from gateway_inventory where owner = $1 order by first_block desc, address) as g")).

-define(HOTSPOT_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_HOTSPOT_LIST_BEFORE,
                           [?SELECT_HOTSPOT_BASE,
                            "where (g.address > $1 and g.first_block = $2) or (g.first_block < $2) ",
                            "order by g.first_block desc, g.address limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)],
                            []),

    {ok, S2} = epgsql:parse(Conn, ?S_HOTSPOT_LIST,
                           [?SELECT_HOTSPOT_BASE,
                            "order by g.first_block desc, g.address limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)],
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_OWNER_HOTSPOT_LIST_BEFORE,
                           [?SELECT_OWNER_HOTSPOT,
                            "where (g.address > $2 and g.first_block = $3) or (g.first_block < $3) limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)],
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_OWNER_HOTSPOT_LIST,
                           ?SELECT_OWNER_HOTSPOT, []),

    {ok, S5} = epgsql:parse(Conn, ?S_HOTSPOT,
                           [?SELECT_HOTSPOT_BASE,
                            "where g.address = $1"], []),

    #{?S_HOTSPOT_LIST_BEFORE => S1,
      ?S_HOTSPOT_LIST => S2,
      ?S_OWNER_HOTSPOT_LIST_BEFORE => S3,
      ?S_OWNER_HOTSPOT_LIST => S4,
      ?S_HOTSPOT => S5}.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_hotspot_list([{owner, undefined} | Args]), block_time);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address), block_time);
handle('GET', [Address, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    ?MK_RESPONSE(bh_route_txns:get_activity_list({hotspot, Address}, Args),
                 ?CACHE_TIME_BLOCK_ALIGNED(Args));
handle('GET', [Address, <<"elections">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_elections:get_election_list({hotspot, Address}, Args), block_time);
handle('GET', [Address, <<"challenges">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_challenges:get_challenge_list({hotspot, Address}, Args), block_time);

handle(_, _, _Req) ->
    ?RESPONSE_404.


get_hotspot_list([{owner, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST, []),
    mk_hotspot_list_from_result(undefined, Result);
get_hotspot_list([{owner, Owner}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST, [Owner]),
    mk_hotspot_list_from_result(undefined, Result);
get_hotspot_list([{owner, Owner}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before_address">> := BeforeAddress,
                <<"before_block">> := BeforeBlock,
                <<"height">> := CursorHeight }} ->
            case Owner of
                undefined ->
                    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(CursorHeight, Result);
                _ ->
                    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST_BEFORE, [Owner, BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(CursorHeight, Result)
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

mk_hotspot_list_from_result(undefined, {ok, _, Results}) ->
    %% no cursor, return a result
    {ok, hotspot_list_to_json(Results), mk_cursor(Results)};
mk_hotspot_list_from_result(CursorHeight,
                            {ok, _, [{Height, _Block, _FirstBlock,
                                      _Address, _Owner, _Location, _Score,
                                      _ShortStreet, _LongStreet,
                                      _ShortCity, _LongCity,
                                      _ShortState, _LongState,
                                      _ShortCountry, _LongCountry} | _]}) when CursorHeight /= Height ->
    {error, badarg};
mk_hotspot_list_from_result(CursorHeight,
                            {ok, _, [{Height, _Block, _FirstBlock,
                                      _Address, _Owner, _Location, _Score,
                                      _ShortStreet, _LongStreet,
                                      _ShortCity, _LongCity,
                                      _ShortState, _LongState,
                                      _ShortCountry, _LongCountry} | _] = Results}) when CursorHeight == Height ->
    %% The above head ensures that the given cursor height matches the
    %% height in the results
    {ok, hotspot_list_to_json(Results), mk_cursor(Results)};
mk_hotspot_list_from_result(_Height, {ok, _, Results}) ->
    %% This really only happens when Result = [], which can happen if
    %% the last page is exactly the right height to allow for another
    %% (empty) last page.
    {ok, hotspot_list_to_json(Results), mk_cursor(Results)}.


mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?HOTSPOT_LIST_LIMIT of
        true -> undefined;
        false ->
            {Height, _ScoreBlock, FirstBlock, Address, _Owner, _Location, _Score,
             _ShortStreet, _LongStreet,
             _ShortCity, _LongCity,
             _ShortState, _LongState,
             _ShortCountry, _LongCountry} = lists:last(Results),
            #{ before_address => Address,
               before_block => FirstBlock,
               height => Height}
    end.

%%
%% to_jaon
%%

hotspot_list_to_json(Results) ->
    lists:map(fun hotspot_to_json/1, Results).

hotspot_to_json({Height, ScoreBlock, _FirstBlock, Address, Owner, Location, Score,
                 ShortStreet, LongStreet,
                 ShortCity, LongCity,
                 ShortState, LongState,
                 ShortCountry, LongCountry}) ->
    {ok, Name} = erl_angry_purple_tiger:animal_name(Address),
    ?INSERT_LAT_LON(Location,
                    #{
                      <<"address">> => Address,
                      <<"name">> => list_to_binary(Name),
                      <<"owner">> => Owner,
                      <<"location">> => Location,
                      <<"geocode">> =>
                          #{
                            <<"short_street">> => ShortStreet,
                            <<"long_street">> => LongStreet,
                            <<"short_city">> => ShortCity,
                            <<"long_city">> => LongCity,
                            <<"short_state">> => ShortState,
                            <<"long_state">> => LongState,
                            <<"short_country">> => ShortCountry,
                            <<"long_country">> => LongCountry
                           },
                      <<"score_update_height">> => ScoreBlock,
                      <<"score">> => Score,
                      <<"block">> => Height
                     }).
