-module(bh_route_accounts).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_account_list/1, get_account/2]).

-define(S_ACCOUNT_LIST_BEFORE, "account_list_before").
-define(S_ACCOUNT_LIST, "account_list").
-define(S_ACCOUNT, "account").
-define(S_ACCOUNT_AT_BLOCK, "account_at_block").
-define(S_ACCOUNT_BALANCE_SERIES, "account_balance_series").
-define(S_ACCOUNT_RICH_LIST, "account_rich_list").
-define(ACCOUNT_LIST_LIMIT, 100).
-define(ACCOUNT_RICH_LIST_LIMIT, 100).

prepare_conn(_Conn) ->
    AccountListLimit = "limit " ++ integer_to_list(?ACCOUNT_LIST_LIMIT),
    Loads = [
        {?S_ACCOUNT_LIST_BEFORE,
            {account_list_base,
                [
                    {with, ""},
                    {height, account_list_height},
                    {extend, account_list_extend},
                    {source, account_inventory_source},
                    {scope, account_list_before_scope},
                    {order, account_list_order},
                    {limit, AccountListLimit}
                ],
                [text, int8, int8]}},
        {?S_ACCOUNT_LIST,
            {account_list_base,
                [
                    {with, ""},
                    {height, account_list_height},
                    {extend, account_list_extend},
                    {source, account_inventory_source},
                    {scope, ""},
                    {order, account_list_order},
                    {limit, AccountListLimit}
                ],
                []}},
        {?S_ACCOUNT,
            {account_list_base,
                [
                    {with, ""},
                    {height, account_list_height},
                    {extend, account_speculative_extend},
                    {source, account_inventory_source},
                    {scope, account_scope},
                    {order, ""},
                    {limit, ""}
                ],
                [text]}},
        {?S_ACCOUNT_AT_BLOCK,
            {account_list_base,
                [
                    {with, account_at_block_with},
                    {height, "l.block as height"},
                    {extend, account_at_block_extend},
                    {source, account_at_block_source},
                    {scope, ""},
                    {order, "order by block desc"},
                    {limit, "limit 1"}
                ],
                [text, int8]}},
        {?S_ACCOUNT_BALANCE_SERIES,
            {?S_ACCOUNT_BALANCE_SERIES, [], [text, timestamptz, interval, text, interval]}},
        {?S_ACCOUNT_RICH_LIST,
            {account_list_base,
                [
                    {with, ""},
                    {height, account_list_height},
                    {extend, account_list_extend},
                    {source, account_inventory_source},
                    {scope, ""},
                    {order, "order by (l.balance + coalesce(l.staked_balance, 0)) desc"},
                    {limit, "limit $1"}
                ],
                [int4]}}
    ],
    bh_db_worker:load_from_eql("accounts.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_account_list(Args), block_time);
handle('GET', [<<"rich">>], Req) ->
    Args = ?GET_ARGS([limit], Req),
    ?MK_RESPONSE(get_account_rich_list(Args), block_time);
handle('GET', [Account], Req) ->
    Args = ?GET_ARGS([max_block], Req),
    ?MK_RESPONSE(get_account(Account, Args), never);
handle('GET', [Account, <<"ouis">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(
        bh_route_ouis:get_oui_list([{owner, Account} | Args]),
        block_time
    );
handle('GET', [Account, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([filter_modes, cursor], Req),
    ?MK_RESPONSE(
        bh_route_hotspots:get_hotspot_list([{owner, Account}, {city, undefined} | Args]),
        block_time
    );
handle('GET', [Account, <<"validators">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(
        bh_route_validators:get_validator_list([{owner, Account} | Args]),
        block_time
    );
handle('GET', [Account, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time, limit, filter_types], Req),
    Result = bh_route_txns:get_activity_list({account, Account}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Account, <<"activity">>, <<"count">>], Req) ->
    Args = ?GET_ARGS([filter_types], Req),
    ?MK_RESPONSE(bh_route_txns:get_activity_count({account, Account}, Args), block_time);
handle('GET', [Account, <<"elections">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time, limit], Req),
    ?MK_RESPONSE(
        bh_route_elections:get_election_list({account, Account}, Args),
        block_time
    );
handle('GET', [Account, <<"challenges">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time, limit], Req),
    ?MK_RESPONSE(
        bh_route_challenges:get_challenge_list({account, Account}, Args),
        block_time
    );
handle('GET', [Account, <<"rewards">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_list({account, Account}, Args), block_time);
handle('GET', [Account, <<"rewards">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_sum({account, Account}, Args), block_time);
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

get_account_rich_list([{limit, BinLimit}]) ->
    Limit =
        case BinLimit of
            undefined -> ?ACCOUNT_RICH_LIST_LIMIT;
            _ -> max(0, min(binary_to_integer(BinLimit), ?ACCOUNT_RICH_LIST_LIMIT))
        end,
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACCOUNT_RICH_LIST, [Limit]),
    {ok, account_list_to_json(Results)}.

get_account_list([{cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_ACCOUNT_LIST, []),
    mk_account_list_from_result(Result);
get_account_list([{cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            Result = ?PREPARED_QUERY(?S_ACCOUNT_LIST_BEFORE, [BeforeAddress, BeforeBlock]),
            mk_account_list_from_result(Result);
        _ ->
            {error, badarg}
    end.

get_account(Account, [{max_block, MaxBlock}]) ->
    try
        {Query, Args} =
            case MaxBlock of
                undefined ->
                    {?S_ACCOUNT, [Account]};
                _ ->
                    Before = binary_to_integer(MaxBlock),
                    {?S_ACCOUNT_AT_BLOCK, [Account, Before]}
            end,
        case ?PREPARED_QUERY(Query, Args) of
            {ok, _, [Result]} ->
                {ok, account_to_json(Result)};
            {ok, _, []} ->
                {ok, account_to_json({null, Account, 0, 0, 0, 0, 0, 0, 0, 0})};
            {error, Error} ->
                {error, Error}
        end
    catch
        error:badarg -> {error, badarg}
    end.

mk_account_list_from_result({ok, _, Results}) ->
    {ok, account_list_to_json(Results), mk_cursor(Results)}.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?ACCOUNT_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {Height, Address, _DCBalance, _DCNonce, _SecBalance, _SecNonce, _Balance,
                _StakedBalance, _Nonce, FirstBlock} = lists:last(Results),
            #{
                before_address => Address,
                before_block => FirstBlock,
                height => Height
            }
    end.

%%
%% Stats
%%

get_stats(Account) ->
    Now = calendar:universal_time(),
    Interval = fun(B) ->
        {ok, {_, V}} = ?PARSE_INTERVAL(B),
        V
    end,
    [BalanceHourlyResults, BalanceWeeklyResults, BalanceMonthlyResults] =
        ?EXECUTE_BATCH([
            %% Hourly: Last 24 hours, truncating start/end to the hour, bucketed
            %by the hour
            {?S_ACCOUNT_BALANCE_SERIES, [
                Account,
                Now,
                Interval(<<"24 hour">>),
                <<"hour">>,
                Interval(<<"1 hour">>)
            ]},
            %% Weekly: Last 1 weekk, truncating start/end to the day, but
            %bucketd every 8 hours
            {?S_ACCOUNT_BALANCE_SERIES, [
                Account,
                Now,
                Interval(<<"1 week">>),
                <<"day">>,
                Interval(<<"8 hour">>)
            ]},
            %% Monthly: Last 30 days, truncating start/end to the day, bucketed
            %every day
            {?S_ACCOUNT_BALANCE_SERIES, [
                Account,
                Now,
                Interval(<<"30 day">>),
                <<"day">>,
                Interval(<<"1 day">>)
            ]}
        ]),
    {ok, #{
        last_day => mk_balance_stats(BalanceHourlyResults),
        last_week => mk_balance_stats(BalanceWeeklyResults),
        last_month => mk_balance_stats(BalanceMonthlyResults)
    }}.

mk_balance_stats({ok, Results}) ->
    lists:map(
        fun({Timestamp, Value}) ->
            #{
                timestamp => iso8601:format(Timestamp),
                balance => Value
            }
        end,
        Results
    ).

%%
%% json
%%

account_list_to_json(Results) ->
    lists:map(fun account_to_json/1, Results).

account_to_json(
    {Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, StakedBalance, Nonce,
        _FirstBlock}
) ->
    #{
        <<"address">> => Address,
        <<"balance">> => Balance,
        <<"staked_balance">> => StakedBalance,
        <<"nonce">> => Nonce,
        <<"dc_balance">> => DCBalance,
        <<"dc_nonce">> => DCNonce,
        <<"sec_balance">> => SecBalance,
        <<"sec_nonce">> => SecNonce,
        <<"block">> => Height
    };
account_to_json(
    {Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, StakedBalance, Nonce,
        FirstBlock, SpecNonce, SpecSecNonce}
) ->
    Base = account_to_json(
        {Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, StakedBalance, Nonce,
            FirstBlock}
    ),
    Base#{
        <<"speculative_nonce">> => SpecNonce,
        <<"speculative_sec_nonce">> => SpecSecNonce
    }.
