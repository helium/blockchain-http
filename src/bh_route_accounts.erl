-module(bh_route_accounts).

-behavior(bh_route_handler).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_account_list/2, get_account/1]).


-define(S_ACCOUNT_LIST_BEFORE, "account_list_before").
-define(S_ACCOUNT_LIST, "account_list").
-define(S_ACCOUNT, "account").

prepare_conn(Conn) ->
    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT_LIST_BEFORE,
                           "select address, dc_balance, dc_nonce, security_balance, security_nonce, balance, nonce from account_ledger  where address < $1 order by block desc, address limit $2", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT_LIST,
                           "select address, dc_balance, dc_nonce, security_balance, security_nonce, balance, nonce from account_ledger order by block desc, address limit $1", []),

    {ok, _} = epgsql:parse(Conn, ?S_ACCOUNT,
                           "select address, dc_balance, dc_nonce, security_balance, security_nonce, balance, nonce from account_ledger where address = $1", []),

    ok.

handle('GET', [], Req) ->
    Before = ?GET_ARG_BEFORE(Req, undefined),
    Limit = ?GET_ARG_LIMIT(Req),
    ?MK_RESPONSE(get_account_list(Before, Limit));
handle('GET', [Account], _Req) ->
    ?MK_RESPONSE(get_account(Account));

handle(_, _, _Req) ->
    ?RESPONSE_404.

get_account_list(undefined, Limit)  ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACCOUNT_LIST, [Limit]),
    {ok, account_list_to_json(Results)};
get_account_list(Before, Limit) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_ACCOUNT_LIST_BEFORE, [Before, Limit]),
    {ok, account_list_to_json(Results)}.

get_account(Account) ->
    {ok, _, [Result]} = ?PREPARED_QUERY(?S_ACCOUNT, [Account]),
    {ok, account_to_json(Result)}.


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
      <<"data_credit_balance">> => DCBalance,
      <<"data_credit_nonce">> => DCNonce,
      <<"security_balance">> => SecBalance,
      <<"security_nonce">> => SecNonce
     }.
