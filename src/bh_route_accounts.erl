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

-define(SELECT_ACCOUNT_BASE(A), "select l.address, l.dc_balance, l.dc_nonce, l.security_balance, l.security_nonce, l.balance, l.nonce" A " from account_ledger l ").
-define(SELECT_ACCOUNT_BASE, ?SELECT_ACCOUNT_BASE("")).

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT_LIST_BEFORE,
                           ?SELECT_ACCOUNT_BASE "where l.address > $1 order by block desc, address limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT_LIST,
                           ?SELECT_ACCOUNT_BASE "order by block desc, address limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT,
                           ?SELECT_ACCOUNT_BASE(
                              ", (select coalesce(max(nonce), l.nonce) from pending_transactions p where p.address = l.address and nonce_type='balance' and status != 'failed') as speculative_nonce"
                             )
                           "where l.address = $1", []),

    ok.

handle('GET', [], Req) ->
    Args = ?GET_ARGS([before, limit], Req),
    ?MK_RESPONSE(get_account_list(Args));
handle('GET', [Account], _Req) ->
    ?MK_RESPONSE(get_account(Account));
handle('GET', [Account, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([before, limit], Req),
    ?MK_RESPONSE(bh_route_hotspots:get_hotspot_list([{owner, Account} | Args]));

handle(_, _, _Req) ->
    ?RESPONSE_404.

get_account_list([{before, undefined}, {limit, Limit}])  ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACCOUNT_LIST, [Limit]),
    {ok, account_list_to_json(Results)};
get_account_list([{before, Before}, {limit, Limit}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACCOUNT_LIST_BEFORE, [Before, Limit]),
    {ok, account_list_to_json(Results)}.

get_account(Account) ->
    case ?PREPARED_QUERY(?S_ACCOUNT, [Account]) of
        {ok, _, [Result]} ->
            {ok, account_to_json(Result)};
        _ ->
            {ok, account_to_json({Account, 0, 0, 0, 0, 0, 0, 0})}
    end.


%%
%% json
%%

account_list_to_json(Results) ->
    lists:map(fun account_to_json/1, Results).

account_to_json({Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce}) ->
    #{
      <<"address">> => Address,
      <<"balance">> => Balance,
      <<"nonce">> => Nonce,
      <<"dc_balance">> => DCBalance,
      <<"dc_nonce">> => DCNonce,
      <<"sec_balance">> => SecBalance,
      <<"sec_nonce">> => SecNonce
     };
account_to_json({Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, SpecNonce}) ->
    Base = account_to_json({Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce}),
    Base#{
          <<"speculative_nonce">> => SpecNonce
         }.
