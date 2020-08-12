-module(bh_route_accounts).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_account_list/1, get_account/1]).


-define(S_ACCOUNT_LIST_BEFORE, "account_list_before").
-define(S_ACCOUNT_LIST, "account_list").
-define(S_ACCOUNT, "account").
-define(S_ACCOUNT_BAL_HOURLY, "account_bal_hourly").
-define(S_ACCOUNT_BAL_MONTHLY, "account_bal_monthly").
-define(S_ACCOUNT_BAL_WEEKLY, "account_bal_weekly").

-define(SELECT_ACCOUNT_BASE(A),
        ["select (select max(height) from blocks) as height, l.address, l.dc_balance, l.dc_nonce, l.security_balance, l.security_nonce, l.balance, l.nonce, l.first_block",
         A, " from account_inventory l "]).
-define(SELECT_ACCOUNT_BASE, ?SELECT_ACCOUNT_BASE("")).

-define(SELECT_ACCOUNT_STATS(TS),
       ["with ts as ( ", TS, " order by timestamp desc),",
        "accounts_ts as (",
        " select accounts.*, blocks.timestamp  from accounts inner join blocks on blocks.height = accounts.block",
        " where address = $1 ",
        ")",
        "select ts.timestamp, (select accounts_ts.balance from accounts_ts where timestamp <= ts.timestamp order by timestamp desc limit 1) from ts"
       ]).

-define(ACCOUNT_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_ACCOUNT_LIST_BEFORE,
                            [?SELECT_ACCOUNT_BASE,
                             "where (l.address > $1 and l.first_block = $2) or (l.first_block < $2) ",
                             "order by first_block desc, address limit ", integer_to_list(?ACCOUNT_LIST_LIMIT)],
                            []),

    {ok, S2} = epgsql:parse(Conn, ?S_ACCOUNT_LIST,
                             [?SELECT_ACCOUNT_BASE,
                              "order by first_block desc, address limit ", integer_to_list(?ACCOUNT_LIST_LIMIT)],
                             []),

    {ok, S3} = epgsql:parse(Conn, ?S_ACCOUNT,
                           [?SELECT_ACCOUNT_BASE(
                               [", (select greatest(l.nonce, coalesce(max(p.nonce), l.nonce)) from pending_transactions p where p.address = l.address and nonce_type='balance' and status != 'failed') as speculative_nonce",
                                ", (select greatest(l.security_nonce, coalesce(max(p.nonce)), l.security_nonce) from pending_transactions p where p.address = l.address and nonce_type='security' and status != 'failed') as speculative_sec_nonce"
                               ]), "where l.address = $1"],
                            []),

    {ok, S4} = epgsql:parse(Conn, ?S_ACCOUNT_BAL_HOURLY,
                            ?SELECT_ACCOUNT_STATS("select generate_series(date_trunc('hour', now() + '1 hour') - '24 hour'::interval, date_trunc('hour', now() + '1 hour'), '1 hour') as timestamp")
                           , []),

    {ok, S5} = epgsql:parse(Conn, ?S_ACCOUNT_BAL_MONTHLY,
                            ?SELECT_ACCOUNT_STATS("select generate_series(date_trunc('day', now() + '1 day') - '30 day'::interval, date_trunc('day', now() + '1 day'), '1 day') as timestamp")
                           , []),

    {ok, S6} = epgsql:parse(Conn, ?S_ACCOUNT_BAL_WEEKLY,
                            ?SELECT_ACCOUNT_STATS("select generate_series(date_trunc('day', now() + '8 hour') - '1 week'::interval, date_trunc('day', now() + '8 hour'), '8 hour') as timestamp")
                           , []),

    #{?S_ACCOUNT_LIST_BEFORE => S1,
      ?S_ACCOUNT_LIST => S2,
      ?S_ACCOUNT => S3,
      ?S_ACCOUNT_BAL_HOURLY => S4,
      ?S_ACCOUNT_BAL_MONTHLY => S5,
      ?S_ACCOUNT_BAL_WEEKLY => S6
     }.

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_account_list(Args), block_time);
handle('GET', [Account], _Req) ->
    ?MK_RESPONSE(get_account(Account), never);
handle('GET', [Account, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_hotspots:get_hotspot_list([{owner, Account}, {city, undefined} | Args]), block_time);
handle('GET', [Account, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    Result = bh_route_txns:get_activity_list({account, Account}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Account, <<"elections">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_elections:get_election_list({account, Account}, Args), block_time);
handle('GET', [Account, <<"challenges">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_challenges:get_challenge_list({account, Account}, Args), block_time);
handle('GET', [Account, <<"pending_transactions">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_pending_txns:get_pending_txn_list({actor, Account}, Args), never);
handle('GET', [Account, <<"stats">>], _Req) ->
    %% Shortest stats period is every hour. To try to keep the stats
    %% as fresh as possible we cache for shorter than an
    %% hour. Starting with 10 block times for now.
    ?MK_RESPONSE(get_stats(Account), {block_time, 10});

handle(_, _, _Req) ->
    ?RESPONSE_404.

get_account_list([{cursor, undefined}])  ->
    Result = ?PREPARED_QUERY(?S_ACCOUNT_LIST, []),
    mk_account_list_from_result(undefined, Result);
get_account_list([{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{ <<"before_address">> := BeforeAddress,
                <<"before_block">> := BeforeBlock,
                <<"height">> := CursorHeight}} ->
            Result = ?PREPARED_QUERY(?S_ACCOUNT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
            mk_account_list_from_result(CursorHeight, Result);
        _ ->
            {error, badarg}
    end.

get_account(Account) ->
    case ?PREPARED_QUERY(?S_ACCOUNT, [Account]) of
        {ok, _, [Result]} ->
            {ok, account_to_json(Result)};
        _ ->
            {ok, account_to_json({null, Account, 0, 0, 0, 0, 0, 0, 0})}
    end.

mk_account_list_from_result(undefined, {ok, _, Results}) ->
    {ok, account_list_to_json(Results), mk_cursor(Results)};
mk_account_list_from_result(CursorHeight, {ok, _, [{Height, _Address,
                                                    _DCBalance, _DCNonce,
                                                    _SecBalance, _SecNonce,
                                                    _Balance, _Nonce,
                                                    _FirstBlock} | _]}) when Height /= CursorHeight ->
    %% For a mismatched height we return a bad argument so the
    %% requester can re-start
    {error, cursor_expired};
mk_account_list_from_result(CursorHeight, {ok, _, [{Height, _Address,
                                                    _DCBalance, _DCNonce,
                                                    _SecBalance, _SecNonce,
                                                    _Balance, _Nonce,
                                                    _FirstBlock} | _] = Results}) when Height == CursorHeight ->
    %% The above head ensures that the given cursor height matches the
    %% height in the results
    {ok, account_list_to_json(Results), mk_cursor(Results)};
mk_account_list_from_result(_Height, {ok, _, Results}) ->
    %% This really only happens when Result = [], which can happen if
    %% the last page is exactly the right height to allow for another
    %% (empty) last page.
    {ok, account_list_to_json(Results), mk_cursor(Results)}.


mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?ACCOUNT_LIST_LIMIT of
        true -> undefined;
        false ->
            {Height, Address, _DCBalance, _DCNonce, _SecBalance, _SecNonce, _Balance, _Nonce, FirstBlock} = lists:last(Results),
            #{ before_address => Address,
               before_block => FirstBlock,
               height => Height
             }
    end.

%%
%% Stats
%%

get_stats(Account) ->
    case lists:foldl(fun(_Query, {error, Error}) ->
                             {error, Error};
                        (Query, {ok, Acc}) ->
                             case ?PREPARED_QUERY(Query, [Account]) of
                                 {ok, _, Results} ->
                                     {ok, [Results | Acc]};
                                 _ ->
                                     {error, not_found}
                             end
                     end,
                     {ok, []},
                     [?S_ACCOUNT_BAL_HOURLY,
                      ?S_ACCOUNT_BAL_WEEKLY,
                      ?S_ACCOUNT_BAL_MONTHLY]
                    ) of
        {ok, [BalanceMonthlyResults, BalanceWeeklyResults, BalanceHourlyResults]} ->
            {ok, #{
                   last_day => mk_balance_stats(BalanceHourlyResults),
                   last_week => mk_balance_stats(BalanceWeeklyResults),
                   last_month => mk_balance_stats(BalanceMonthlyResults)
                  }};
        {error, Error} ->
            {error, Error}
    end.


mk_balance_stats(Results) ->
    lists:map(fun({Timestamp, Value}) ->
                      #{ timestamp => iso8601:format(Timestamp),
                         balance => Value
                       }
              end, Results).

%%
%% json
%%

account_list_to_json(Results) ->
    lists:map(fun account_to_json/1, Results).

account_to_json({Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, _FirstBlock}) ->
    #{
      <<"address">> => Address,
      <<"balance">> => Balance,
      <<"nonce">> => Nonce,
      <<"dc_balance">> => DCBalance,
      <<"dc_nonce">> => DCNonce,
      <<"sec_balance">> => SecBalance,
      <<"sec_nonce">> => SecNonce,
      <<"block">> => Height
     };
account_to_json({Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, FirstBlock,
                 SpecNonce, SpecSecNonce}) ->
    Base = account_to_json({Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, FirstBlock}),
    Base#{
          <<"speculative_nonce">> => SpecNonce,
          <<"speculative_sec_nonce">> => SpecSecNonce
         }.
