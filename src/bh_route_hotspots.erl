-module(bh_route_hotspots).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_hotspot_list/1,
         get_hotspot/1,
         hotspot_to_geo_json/1]).


-define(S_HOTSPOT_LIST_BEFORE, "hotspot_list_before").
-define(S_HOTSPOT_LIST, "hotspot_list").
-define(S_OWNER_HOTSPOT_LIST_BEFORE, "owner_hotspot_list_before").
-define(S_OWNER_HOTSPOT_LIST, "owner_hotspot_list").
-define(S_HOTSPOT, "hotspot").
-define(S_HOTSPOT_LIST_CITY, "hotspot_list_city").
-define(S_HOTSPOT_LIST_CITY_BEFORE, "hotspor_list_city_before").
-define(S_CITY_HOTSPOT_LIST, "hotspot_city_list").
-define(S_CITY_HOTSPOT_LIST_BEFORE, "hotspor_city_list_before").

-define(SELECT_HOTSPOT_BASE(G),
        ["select (select max(height) from blocks) as height, ",
         "g.last_block, g.first_block, g.address, g.owner, g.location, g.score, g.nonce, ",
         "s.online as online_status, s.gps as gps_status, s.block as block_status, "
         "l.short_street, l.long_street, l.short_city, l.long_city, l.short_state, l.long_state, l.short_country, l.long_country ",
         G,
         " left join locations l on g.location = l.location ",
         " left join gateway_status s on s.address = g.address "
        ]).
-define(SELECT_HOTSPOT_BASE, ?SELECT_HOTSPOT_BASE("from gateway_inventory g")).
-define(SELECT_OWNER_HOTSPOT,
        ?SELECT_HOTSPOT_BASE(["from (select * from gateway_inventory"
                              "      where owner = $1 order by first_block desc, address) as g"])).
-define(SELECT_HOTSPOT_CITY_BASE(G),
        ["select last(l.short_city), l.long_city, last(l.short_state), last(l.long_state), last(l.short_country), last(l.long_country), count(*) ",
         "from locations l inner join gateway_inventory g on g.location = l.location "
         "where l.search_city like lower($1) ",
         G,
         "group by l.long_city "
         "order by long_city ",
         "limit ", integer_to_list(?HOTSPOT_LIST_CITY_LIMIT)
        ]
       ).

-define(SELECT_CITY_HOTSPOT,
       [?SELECT_HOTSPOT_BASE,
        " where l.long_city = $1"
       ]).
-define(HOTSPOT_LIST_LIMIT, 100).
-define(HOTSPOT_LIST_CITY_LIMIT, 100).

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
                            "where (g.address > $2 and g.first_block = $3) or (g.first_block < $3) ",
                            "order by g.address "
                            "limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)],
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_OWNER_HOTSPOT_LIST,
                           ?SELECT_OWNER_HOTSPOT, []),

    {ok, S5} = epgsql:parse(Conn, ?S_HOTSPOT,
                           [?SELECT_HOTSPOT_BASE,
                            "where g.address = $1"], []),

    {ok, S6} = epgsql:parse(Conn, ?S_HOTSPOT_LIST_CITY,
                            ?SELECT_HOTSPOT_CITY_BASE("")
                           , []),

    {ok, S7} = epgsql:parse(Conn, ?S_HOTSPOT_LIST_CITY_BEFORE,
                            ?SELECT_HOTSPOT_CITY_BASE(" and l.long_city > $2")
                           , []),

    {ok, S8} = epgsql:parse(Conn, ?S_CITY_HOTSPOT_LIST_BEFORE,
                            [?SELECT_HOTSPOT_BASE,
                             "where l.long_city = $1 ",
                             "and (g.address > $2 and g.first_block = $3) or (g.first_block < $3) ",
                             "order by g.address "
                             "limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)
                           ], []),

    {ok, S9} = epgsql:parse(Conn, ?S_CITY_HOTSPOT_LIST,
                            [?SELECT_HOTSPOT_BASE,
                             "where l.long_city = $1 ",
                             "order by g.first_block desc, g.address limit ", integer_to_list(?HOTSPOT_LIST_LIMIT)],
                            []),


    #{?S_HOTSPOT_LIST_BEFORE => S1,
      ?S_HOTSPOT_LIST => S2,
      ?S_OWNER_HOTSPOT_LIST_BEFORE => S3,
      ?S_OWNER_HOTSPOT_LIST => S4,
      ?S_HOTSPOT => S5,
      ?S_HOTSPOT_LIST_CITY => S6,
      ?S_HOTSPOT_LIST_CITY_BEFORE => S7,
      ?S_CITY_HOTSPOT_LIST_BEFORE => S8,
      ?S_CITY_HOTSPOT_LIST => S9
     }.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_hotspot_list([{owner, undefined}, {city, undefined} | Args]), block_time);
handle('GET', [<<"cities">>], Req) ->
    Args = ?GET_ARGS([search, cursor], Req),
    Result = get_hotspot_city_list(Args),
    ?MK_RESPONSE(Result, block_time);
handle('GET', [<<"cities">>, City], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    Result = get_hotspot_list([{owner, undefined}, {city, elli_request:uri_decode(City)} | Args]),
    ?MK_RESPONSE(Result, block_time);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address), block_time);
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

handle(_, _, _Req) ->
    ?RESPONSE_404.


get_hotspot_city_list([{search, Search}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_CITY, [search_format(Search)]),
    mk_hotspot_city_list_from_result(undefined, Result);
get_hotspot_city_list([{search, _Search}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{<<"before_city">> := BeforeCity }=C} ->
            Search = maps:get(<<"search">>, C, undefined),
            Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_CITY_BEFORE, [search_format(Search), BeforeCity]),
            mk_hotspot_city_list_from_result(Search, Result);
        _ ->
            {error, badarg}
    end.


get_hotspot_list([{owner, undefined}, {city, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST, []),
    mk_hotspot_list_from_result(undefined, Result);
get_hotspot_list([{owner, Owner}, {city, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST, [Owner]),
    mk_hotspot_list_from_result(undefined, Result);
get_hotspot_list([{owner, undefined}, {city, City}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_CITY_HOTSPOT_LIST, [City]),
    mk_hotspot_list_from_result(undefined, Result);

get_hotspot_list([{owner, Owner}, {city, City}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before_address">> := BeforeAddress,
                <<"before_block">> := BeforeBlock,
                <<"height">> := CursorHeight }} ->
            case {Owner, City}of
                {undefined, undefined} ->
                    Result = ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(CursorHeight, Result);
                {Owner, undefined} ->
                    Result = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST_BEFORE, [Owner, BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(CursorHeight, Result);
                {undefined, City} ->
                    Result = ?PREPARED_QUERY(?S_CITY_HOTSPOT_LIST_BEFORE, [City, BeforeAddress, BeforeBlock]),
                    mk_hotspot_list_from_result(CursorHeight, Result);
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

search_format(undefined) ->
    <<"%">>;
search_format(Search) ->
    Escaped = lists:foldl(fun({C, Replace}, Acc) ->
                                  binary:replace(Acc, C, Replace, [global])
                          end, Search,
                          [
                           {<<"%">>, <<"\%">>},
                           {<<"_">>, <<"\_">>}
                          ]),
    <<"%", Escaped/binary, "%">>.

mk_hotspot_list_from_result(undefined, {ok, _, Results}) ->
    %% no cursor, return a result
    {ok, hotspot_list_to_json(Results), mk_cursor(Results)};
mk_hotspot_list_from_result(CursorHeight,
                            {ok, _, [
                                     {Height, _Block, _FirstBlock,
                                      _Address, _Owner, _Location,
                                      _Score, _Nonce,
                                      _OnlineStatus, _GPSStatus, _BlockStatus,
                                      _ShortStreet, _LongStreet,
                                      _ShortCity, _LongCity,
                                      _ShortState, _LongState,
                                      _ShortCountry, _LongCountry
                                     } | _]}) when CursorHeight /= Height ->
    {error, cursor_expired};
mk_hotspot_list_from_result(CursorHeight,
                            {ok, _, [{Height, _Block, _FirstBlock,
                                      _Address, _Owner, _Location,
                                      _Score, _Nonce,
                                      _ShortStreet, _LongStreet,
                                      _OnlineStatus, _GPSStatus, _BlockStatus,
                                      _ShortCity, _LongCity,
                                      _ShortState, _LongState,
                                      _ShortCountry, _LongCountry
                                     } | _] = Results}) when CursorHeight == Height ->
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
            case lists:last(Results) of
                {Height, _ScoreBlock, FirstBlock, Address, _Owner, _Location,
                 _Score, _Nonce,
                 _OnlineStatus, _GPSStatus, _BlockStatus,
                 _ShortStreet, _LongStreet,
                 _ShortCity, _LongCity,
                 _ShortState, _LongState,
                 _ShortCountry, _LongCountry
                } ->
                    #{ before_address => Address,
                       before_block => FirstBlock,
                       height => Height
                     }
            end
    end.

mk_hotspot_city_list_from_result(Search, {ok, _, Results}) ->
    {ok, hotspot_city_list_to_json(Results), mk_city_list_cursor(Search, Results)}.

mk_city_list_cursor(Search, Results) when is_list(Results) ->
    case length(Results) < ?HOTSPOT_LIST_CITY_LIMIT of
        true ->
            undefined;
        false ->
            {_ShortCity, LongCity,
             _ShortState, _LongState,
             _ShortCountry, _LongCountry, _Count} = lists:last(Results),
            Base = #{ before_city => LongCity },
            case Search of
                undefined -> Base;
                _ -> Base#{ search => Search }
            end
    end.

%%
%% to_jaon
%%

hotspot_list_to_json(Results) ->
    lists:map(fun hotspot_to_json/1, Results).

hotspot_city_list_to_json(Results) ->
    lists:map(fun hotspot_city_to_json/1, Results).


hotspot_to_geo_json({ShortStreet, LongStreet,
                     ShortCity, LongCity,
                     ShortState, LongState,
                     ShortCountry, LongCountry}) ->
    Base = hotspot_to_geo_json({ShortCity, LongCity,
                                ShortState, LongState,
                                ShortCountry, LongCountry}),
    Base#{
          short_street => ShortStreet,
          long_street => LongStreet
         };
hotspot_to_geo_json({ShortCity, LongCity,
                     ShortState, LongState,
                     ShortCountry, LongCountry}) ->
    #{
      short_city => ShortCity,
      long_city => LongCity,
      short_state => ShortState,
      long_state => LongState,
      short_country => ShortCountry,
      long_country => LongCountry
     }.

hotspot_to_json({Height, ScoreBlock, FirstBlock, Address, Owner, Location,
                 Score, Nonce,
                 OnlineStatus, GPSStatus, BlockStatus,
                 ShortStreet, LongStreet,
                 ShortCity, LongCity,
                 ShortState, LongState,
                 ShortCountry, LongCountry
                }) ->

    MaybeZero = fun(null) -> 0;
                   (V) -> V
                end,
    {ok, Name} = erl_angry_purple_tiger:animal_name(Address),
    ?INSERT_LAT_LON(Location,
                    #{
                      address => Address,
                      name => list_to_binary(Name),
                      owner => Owner,
                      location => Location,
                      geocode => hotspot_to_geo_json({ShortStreet, LongStreet,
                                                      ShortCity, LongCity,
                                                      ShortState, LongState,
                                                      ShortCountry, LongCountry}),
                      score_update_height => ScoreBlock,
                      score => Score,
                      block_added => FirstBlock,
                      block => Height,
                      status =>
                          #{
                            online => OnlineStatus,
                            gps => GPSStatus,
                            height => BlockStatus
                           },
                      nonce => MaybeZero(Nonce)
                     }).

hotspot_city_to_json({ShortCity, LongCity,
                      ShortState, LongState,
                      ShortCountry, LongCountry,
                      Count}) ->
    Base = hotspot_to_geo_json({ShortCity, LongCity,
                                ShortState, LongState,
                                ShortCountry, LongCountry}),
    Base#{ count => Count }.
