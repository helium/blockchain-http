-module(bh_route_elections).

-export([prepare_conn/1, handle/3]).
-export([get_election_list/2]).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-define(S_ELECTION_TXN_LIST, "election_txn_list").
-define(S_ELECTION_TXN_LIST_BEFORE, "election_txn_list_before").
-define(S_ELECTION_ACTOR_TXN_LIST, "election_actor_txn_list").
-define(S_ELECTION_ACTOR_TXN_LIST_BEFORE, "election_actor_txn_list_before").
-define(S_ELECTION_ACCOUNT_TXN_LIST, "election_account_txn_list").
-define(S_ELECTION_ACCOUNT_TXN_LIST_BEFORE, "election_account_txn_list_before").

-define(SELECT_ELECTION_TXN_LIST_BASE,
       [?SELECT_TXN_FIELDS("txn_filter_actor_activity(t.actor, t.type, t.fields) as fields"),
        "from ",
        "(select tr.*,'undefined' as actor from transactions tr where type = 'consensus_group_v1' order by block desc) t "]).

-define(SELECT_ELECTION_ACTOR_TXN_LIST_BASE,
       [?SELECT_TXN_FIELDS("txn_filter_actor_activity(t.actor, t.type, t.fields) as fields"),
        "from ",
        "(select tr.*, a.actor from",
        " transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash",
        " where tr.type = 'consensus_group_v1' and a.actor=$1 order by block desc) t "]).

-define(SELECT_ELECTION_ACCOUNT_TXN_LIST_BASE,
       [?SELECT_TXN_FIELDS("txn_filter_actor_activity(t.actor, t.type, t.fields) as fields"),
        "from ",
        "(select tr.*, a.actor from",
        " transaction_actors a inner join transactions tr on a.transaction_hash = tr.hash",
        " where tr.type = 'consensus_group_v1' and a.actor in",
        " (select address from gateway_ledger where owner = $1) ",
        "order by block desc) t "]).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_ELECTION_TXN_LIST,
                            [?SELECT_ELECTION_TXN_LIST_BASE,
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                             []),

    {ok, S2} = epgsql:parse(Conn, ?S_ELECTION_TXN_LIST_BEFORE,
                            [?SELECT_ELECTION_TXN_LIST_BASE,
                             "where t.block < $1",
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                            []),

    {ok, S3} = epgsql:parse(Conn, ?S_ELECTION_ACTOR_TXN_LIST,
                            [?SELECT_ELECTION_ACTOR_TXN_LIST_BASE,
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                             []),

    {ok, S4} = epgsql:parse(Conn, ?S_ELECTION_ACTOR_TXN_LIST_BEFORE,
                            [?SELECT_ELECTION_ACTOR_TXN_LIST_BASE,
                             "where t.block < $2",
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                            []),

    {ok, S5} = epgsql:parse(Conn, ?S_ELECTION_ACCOUNT_TXN_LIST,
                            [?SELECT_ELECTION_ACCOUNT_TXN_LIST_BASE,
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                             []),

    {ok, S6} = epgsql:parse(Conn, ?S_ELECTION_ACCOUNT_TXN_LIST_BEFORE,
                            [?SELECT_ELECTION_ACCOUNT_TXN_LIST_BASE,
                             "where t.block < $2",
                             "limit ", integer_to_list(?ELECTION_TXN_LIST_LIMIT)
                            ],
                            []),

    #{
      ?S_ELECTION_TXN_LIST => S1,
      ?S_ELECTION_TXN_LIST_BEFORE => S2,
      ?S_ELECTION_ACTOR_TXN_LIST => S3,
      ?S_ELECTION_ACTOR_TXN_LIST_BEFORE => S4,
      ?S_ELECTION_ACCOUNT_TXN_LIST => S5,
      ?S_ELECTION_ACCOUNT_TXN_LIST_BEFORE => S6
     }.


handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_election_list(undefined, Args), block_time).


get_election_list({hotspot, Address}, Args) ->
    get_election_list([Address], {?S_ELECTION_ACTOR_TXN_LIST, ?S_ELECTION_ACTOR_TXN_LIST_BEFORE}, Args);
get_election_list({account, Address}, Args) ->
    get_election_list([Address], {?S_ELECTION_ACCOUNT_TXN_LIST, ?S_ELECTION_ACCOUNT_TXN_LIST_BEFORE}, Args);
get_election_list(undefined, Args) ->
    get_election_list([], {?S_ELECTION_TXN_LIST, ?S_ELECTION_TXN_LIST_BEFORE}, Args).

get_election_list(Args, {StartQuery, _CursorQuery}, [{cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(StartQuery, Args),
    mk_election_list_from_result(Result);
get_election_list(Args, {_StartQuery, CursorQuery}, [{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before">> := Block }} ->
            Result = ?PREPARED_QUERY(CursorQuery, Args ++ [Block]),
            mk_election_list_from_result(Result);
        _ ->
            {error, badarg}
    end.

mk_election_list_from_result({ok, _, Results}) ->
    {ok, ?TXN_LIST_TO_JSON(Results), mk_election_list_cursor(Results)}.

mk_election_list_cursor(Results) ->
    case length(Results) < ?ELECTION_TXN_LIST_LIMIT of
        true -> undefined;
        false ->
            {Height, _Time, _Hash, _Type, _Fields} = lists:last(Results),
            #{ before => Height}
    end.
