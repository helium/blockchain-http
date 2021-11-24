-module(bh_route_snapshots).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_snapshot_list/1]).

-define(S_SNAPSHOT_LIST, "snapshot_list").
-define(S_SNAPSHOT_LIST_BEFORE, "snapshot_list_before").
-define(S_SNAPSHOT_CURRENT, "snapshot_curent").

prepare_conn(_Conn) ->
    SnapshotListLimit = integer_to_list(?SNAPSHOT_LIST_LIMIT),
    Loads = [
        {?S_SNAPSHOT_LIST, {
            {snapshot_list_base,
                [
                    {scope, ""},
                    {limit, "limit " ++ SnapshotListLimit}
                ],
                []}
        }},
        {?S_SNAPSHOT_LIST_BEFORE, {
            {snapshot_list_base,
                [
                    {scope, snapshot_list_before_scope},
                    {limit, "limit " ++ SnapshotListLimit}
                ],
                [int8]}
        }},
        {?S_SNAPSHOT_CURRENT,
            {snapshot_list_base,
                [
                    {scope, ""},
                    {limit, "limit 1"}
                ],
                []}}
    ],
    bh_db_worker:load_from_eql("snapshots.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    Result = get_snapshot_list(Args),
    CacheTime = get_snapshot_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [<<"current">>], _Req) ->
    Result = get_snapshot_current(),
    ?MK_RESPONSE(Result, block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_snapshot_list([{cursor, undefined}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_SNAPSHOT_LIST, []),
    {ok, snapshot_list_to_json(Results), mk_snapshot_list_cursor(Results)};
get_snapshot_list([{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{<<"before">> := Before}} ->
            {ok, _, Results} = ?PREPARED_QUERY(?S_SNAPSHOT_LIST_BEFORE, [Before]),
            {ok, snapshot_list_to_json(Results), mk_snapshot_list_cursor(Results)};
        _ ->
            {error, badarg}
    end.

get_snapshot_current() ->
    case ?PREPARED_QUERY(?S_SNAPSHOT_CURRENT, []) of
        {ok, _, [Result]} ->
            {ok, snapshot_to_json(Result)};
        _ ->
            {error, not_found}
    end.

mk_snapshot_list_cursor(Results) when is_list(Results) ->
    case length(Results) of
        0 ->
            undefined;
        _ ->
            case lists:last(Results) of
                {Height, _SnapshotHash} when Height == 1 -> undefined;
                {Height, _SnapshotHash} -> #{before => Height}
            end
    end.

get_snapshot_list_cache_time({ok, _, undefined}) ->
    %% End of cursor data
    infinity;
get_snapshot_list_cache_time({ok, _, _}) ->
    block_time;
get_snapshot_list_cache_time(_) ->
    never.

%%
%% json
%%

snapshot_list_to_json(Results) ->
    lists:map(fun snapshot_to_json/1, Results).

snapshot_to_json({Height, Hash}) ->
    #{
        block => Height,
        snapshot_hash => Hash
    }.
