-module(bh_route_vars).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_var_list/1, get_var/1]).

-define(MAX_KEY_NAMES, 50).

-define(S_VAR_LIST, "var_list").
-define(S_VAR_LIST_NAMED, "var_list_named").
-define(S_VAR, "var_get").

prepare_conn(_Conn) ->
    Loads = [
        ?S_VAR_LIST,
        {?S_VAR_LIST_NAMED, {?S_VAR_LIST_NAMED, [], [{array, text}]}},
        {?S_VAR, {?S_VAR, [], [text]}}
    ],
    bh_db_worker:load_from_eql(
        "vars.sql",
        Loads
    ).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([keys], Req),
    ?MK_RESPONSE(get_var_list(Args), block_time);
handle('GET', [<<"activity">>], Req) ->
    Args = add_filter_types(?GET_ARGS([cursor, max_time, min_time, limit], Req)),
    Result = bh_route_txns:get_txn_list(Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Name], _Req) ->
    ?MK_RESPONSE(get_var(Name), block_time);
handle(_, _, _Req) ->
    ?RESPONSE_404.

add_filter_types(Args) ->
    Args ++ [{filter_types, <<"vars_v1">>}].

get_var_list([{keys, undefined}]) ->
    {ok, _, Results} = ?PREPARED_QUERY(?S_VAR_LIST, []),
    {ok, var_list_to_json(Results)};
get_var_list([{keys, KeyBins}]) ->
    case binary:split(KeyBins, <<",">>, [global]) of
        [] ->
            {error, badarg};
        KeyNames when length(KeyNames) > ?MAX_KEY_NAMES ->
            {error, badarg};
        KeyNames ->
            KeysArg = lists:flatten([<<"{">>, lists:join(<<",">>, KeyNames), <<"}">>]),
            {ok, _, Results} = ?PREPARED_QUERY(?S_VAR_LIST_NAMED, [KeysArg]),
            {ok, var_list_to_json(Results)}
    end.

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
var_to_json({<<"staking_keys">> = Name, <<"binary">>, Value}) ->
    {Name, b64_to_keys(Value)};
var_to_json({<<"price_oracle_public_keys">> = Name, <<"binary">>, Value}) ->
    {Name, b64_to_keys(Value)};
var_to_json({<<"staking_keys_to_mode_mappings">> = Name, <<"binary">>, Value}) ->
    {Name, maps:from_list([{?BIN_TO_B58(Key), Mode} || {Key, Mode} <- b64_to_props(Value, 8)])};
var_to_json({<<"hip17_res_", _/binary>> = Name, <<"binary">>, Value}) ->
    {Name, [binary_to_integer(N) || N <- string:split(?B64_TO_BIN(Value), ",", all)]};
var_to_json({Name, <<"binary">>, Value}) ->
    {Name, Value}.

b64_to_keys(Value) ->
    Bin = ?B64_TO_BIN(Value),
    BinKeys = [Key || <<Len:8/unsigned-integer, Key:Len/binary>> <= Bin],
    [?BIN_TO_B58(Key) || Key <- BinKeys].

b64_to_props(B64Value, Size) ->
    Bin = ?B64_TO_BIN(B64Value),
    [
        {Key, Value}
     || <<KeyLen:Size/unsigned-integer, Key:KeyLen/binary, ValueLen:Size/unsigned-integer,
            Value:ValueLen/binary>> <= Bin
    ].
