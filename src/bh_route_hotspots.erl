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

-define(SELECT_HOTSPOT_BASE, "select l.block, l.address, l.owner, l.location, l.score from gateway_ledger l ").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT_LIST_BEFORE,
                           ?SELECT_HOTSPOT_BASE "where l.address > $1 order by block desc, address limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT_LIST,
                           ?SELECT_HOTSPOT_BASE "order by block desc, address limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_OWNER_HOTSPOT_LIST_BEFORE,
                           ?SELECT_HOTSPOT_BASE "where l.owner = $1 and l.address > $2 order by block desc, address limit $3", []),

    {ok, _} = epgsql:parse(Conn, ?S_OWNER_HOTSPOT_LIST,
                           ?SELECT_HOTSPOT_BASE "where l.owner = $1 order by block desc, address limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT,
                           ?SELECT_HOTSPOT_BASE "where l.address = $1", []),

    ok.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([owner, before, limit], Req),
    ?MK_RESPONSE(get_hotspot_list(Args));
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address));

handle(_, _, _Req) ->
    ?RESPONSE_404.


get_hotspot_list([{owner, undefined}, {before, undefined}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_HOTSPOT_LIST, [Limit]),
    {ok, hotspot_list_to_json(Results)};
get_hotspot_list([{owner, undefined}, {before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [Before, Limit]),
    {ok, hotspot_list_to_json(Results)};
get_hotspot_list([{owner, Owner}, {before, undefined}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST, [Owner, Limit]),
    {ok, hotspot_list_to_json(Results)};
get_hotspot_list([{owner, Owner}, {before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_OWNER_HOTSPOT_LIST_BEFORE, [Owner, Before, Limit]),
    {ok, hotspot_list_to_json(Results)}.


get_hotspot(Address) ->
    case ?PREPARED_QUERY(?S_HOTSPOT, [Address]) of
        {ok, _, [Result]} ->
            {ok, hotspot_to_json(Result)};
        _ ->
            {error, not_found}
    end.


%%
%% to_jaon
%%

hotspot_list_to_json(Results) ->
    lists:map(fun hotspot_to_json/1, Results).

hotspot_to_json({Block, Address, Owner, Location, Score}) ->
    {ok, Name} = erl_angry_purple_tiger:animal_name(Address),
    ?INSERT_LAT_LON(Location,
                    #{
                      <<"address">> => Address,
                      <<"name">> => list_to_binary(Name),
                      <<"owner">> => Owner,
                      <<"location">> => Location,
                      <<"score_update_height">> => Block,
                      <<"score">> => Score
                     }).
