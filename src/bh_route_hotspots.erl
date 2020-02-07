-module(bh_route_hotspots).

-behavior(bh_route_handler).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_hotspot_list/2, get_hotspot/1]).


-define(S_HOTSPOT_LIST_BEFORE, "hotspot_list_before").
-define(S_HOTSPOT_LIST, "hotspot_list").
-define(S_HOTSPOT, "hotspot").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT_LIST_BEFORE,
                           "select block, address, owner, location, score from gateway_ledger  where address < $1 order by block desc, address limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT_LIST,
                           "select block, address, owner, location, score from gateway_ledger order by block desc, address limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_HOTSPOT,
                           "select block, address, owner, location, score from gateway_ledger where address = $1", []),

    ok.

handle('GET', [], Req) ->
    Before = ?GET_ARG_BEFORE(Req, undefined),
    Limit = ?GET_ARG_LIMIT(Req),
    ?MK_RESPONSE(get_hotspot_list(Before, Limit));
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_hotspot(Address));

handle(_, _, _Req) ->
    ?RESPONSE_404.

get_hotspot_list(undefined, Limit)  ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_HOTSPOT_LIST, [Limit]),
    {ok, hotspot_list_to_json(Results)};
get_hotspot_list(Before, Limit) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_HOTSPOT_LIST_BEFORE, [Before, Limit]),
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
    ?INSERT_LAT_LON(Location,
                    #{
                      <<"address">> => Address,
                      <<"owner">> => Owner,
                      <<"location">> => Location,
                      <<"score_update_height">> => Block,
                      <<"score">> => Score
                     }).
