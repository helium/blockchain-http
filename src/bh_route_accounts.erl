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

-define(SELECT_ACCOUNT_BASE(A), "select (select max(height) from blocks) as height, l.address, l.dc_balance, l.dc_nonce, l.security_balance, l.security_nonce, l.balance, l.nonce, l.first_block" A " from account_ledger l ").
-define(SELECT_ACCOUNT_BASE, ?SELECT_ACCOUNT_BASE("")).

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
                              ", (select coalesce(max(nonce), l.nonce) from pending_transactions p where p.address = l.address and nonce_type='balance' and status != 'failed') as speculative_nonce"
                             ), "where l.address = $1"],
                            []),

    #{?S_ACCOUNT_LIST_BEFORE => S1,
      ?S_ACCOUNT_LIST => S2,
      ?S_ACCOUNT => S3 }.

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_account_list(Args));
handle('GET', [Account], _Req) ->
    ?MK_RESPONSE(get_account(Account));
handle('GET', [Account, <<"hotspots">>], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(bh_route_hotspots:get_hotspot_list([{owner, Account} | Args]));

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
mk_account_list_from_result(Height, {ok, _, [{Height, _Address,
                                             _DCBalance, _DCNonce,
                                             _SecBalance, _SecNonce,
                                             _Balance, _Nonce,
                                             _FirstBlock} | _] = Results}) ->
    %% The above head ensures that the given cursor height matches the
    %% height in the results
    {ok, account_list_to_json(Results), mk_cursor(Results)};
mk_account_list_from_result(_Height, {ok, _, _}) ->
    %% For a mismatched height we return a bad argument so the
    %% requester can re-start
    {error, badarg}.


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
account_to_json({Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, FirstBlock, SpecNonce}) ->
    Base = account_to_json({Height, Address, DCBalance, DCNonce, SecBalance, SecNonce, Balance, Nonce, FirstBlock}),
    Base#{
          <<"speculative_nonce">> => SpecNonce
         }.
