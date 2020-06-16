-module(bh_route_vars).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_var_list/0, get_var/1]).


-define(S_VAR_LIST, "var_list").
-define(S_VAR, "var_get").

-define(SELECT_VARS_BASE, "select v.name, v.type, v.value from vars_inventory v ").

prepare_conn(Conn) ->
    {ok, S1} = epgsql:parse(Conn, ?S_VAR_LIST,
                            [?SELECT_VARS_BASE,
                             "order by name"
                            ], []),

    {ok, S2} = epgsql:parse(Conn, ?S_VAR,
                            [?SELECT_VARS_BASE,
                             "where v.name = $1"
                            ], []),

    #{?S_VAR_LIST => S1,
      ?S_VAR => S2
     }.

handle('GET', [], _Req) ->
    ?MK_RESPONSE(get_var_list(), block_time);
handle('GET', [<<"activity">>], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Name], _Req) ->
    ?MK_RESPONSE(get_var(Name), block_time);

handle(_, _, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"vars_v1">>}].

get_var_list()  ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_VAR_LIST, []),
    {ok, var_list_to_json(Results)}.

get_var(Name) ->
    case ?PREPARED_QUERY(?S_VAR, [Name]) of
        {ok, _, [Result]} ->
            {_, Value} = var_to_json(Result),
            {ok, Value};
        _ ->
            {error, not_found}
    end.


%%
%% json
%%

var_list_to_json(Results) ->
    maps:from_list(lists:map(fun var_to_json/1, Results)).

var_to_json({Name, <<"integer">>, Value}) ->
    {Name, binary_to_integer(Value)};
var_to_json({Name, <<"float">>, Value}) ->
    {Name, binary_to_float(Value)};
var_to_json({Name, <<"atom">>, <<"true">>}) ->
    {Name, true};
var_to_json({Name, <<"atom">>, <<"false">>}) ->
    {Name, false};
var_to_json({Name, <<"atom">>, Value}) ->
    {Name, Value};
var_to_json({<<"price_oracle_public_keys">>=Name, <<"binary">>, Value}) ->
    Bin = ?B64_TO_BIN(Value),
    BinKeys = [ Key || << Len:8/unsigned-integer, Key:Len/binary >> <= Bin ],
    Keys = [?BIN_TO_B58(Key) || Key <- BinKeys],
    {Name, Keys};
var_to_json({Name, <<"binary">>, Value}) ->
    {Name, Value}.
