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

-define(SELECT_SNAPSHOT_BASE, "select b.height, b.snapshot_hash from blocks b ").


prepare_conn(Conn) ->
    SnapshotListLimit = integer_to_list(?SNAPSHOT_LIST_LIMIT),
    {ok, S1} = epgsql:parse(Conn, ?S_SNAPSHOT_LIST,
                            [?SELECT_SNAPSHOT_BASE
                             "where b.snapshot_hash is not null and b.snapshot_hash != '' ",
                             "order by height desc limit ",
                            "(select coalesce(nullif(max(height) % ", SnapshotListLimit, ", 0), ", SnapshotListLimit, ") from blocks)"

                            ], []),

    {ok, S2} = epgsql:parse(Conn, ?S_SNAPSHOT_LIST_BEFORE,
                            [?SELECT_SNAPSHOT_BASE
                             "where b.snapshot_hash is not null and b.snapshot_hash != '' ",
                             "and b.height < $1"
                             "order by height desc limit ", SnapshotListLimit
                            ], []),

    {ok, S3} = epgsql:parse(Conn, ?S_SNAPSHOT_CURRENT,
                            [?SELECT_SNAPSHOT_BASE,
                             "where b.snapshot_hash is not null and b.snapshot_hash != '' ",
                             "order by height desc ",
                             "limit 1"
                            ], []),

    #{
      ?S_SNAPSHOT_LIST => S1,
      ?S_SNAPSHOT_LIST_BEFORE => S2,
      ?S_SNAPSHOT_CURRENT => S3
     }.

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
        {ok, #{ <<"before">> := Before}} ->
            {ok, _, Results} = ?PREPARED_QUERY(?S_SNAPSHOT_LIST_BEFORE, [Before - (Before rem ?SNAPSHOT_LIST_LIMIT)]),
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
        0 -> undefined;
        _ -> case lists:last(Results) of
                 {Height, _SnapshotHash} when Height == 1 -> undefined;
                 {Height, _SnapshotHash}  -> #{ before => Height}
             end
    end.

get_snapshot_list_cache_time({ok, _, undefined}) ->
    infinity;
get_snapshot_list_cache_time({ok, _, #{before := Height}}) ->
    case (Height rem ?SNAPSHOT_LIST_LIMIT) == 0 of
        true -> infinity;
        false -> block_time
    end;
get_snapshot_list_cache_time(_) ->
    never.

%%
%% json
%%

snapshot_list_to_json(Results) ->
    lists:map(fun snapshot_to_json/1, Results).

snapshot_to_json({Height, Hash}) ->
    #{ block => Height,
       snapshot_hash => Hash}.
