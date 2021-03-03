-module(bh_route_validators).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_validator_list/1, get_validator/1]).

-define(S_VALIDATOR_LIST_BEFORE, "validator_list_before").
-define(S_VALIDATOR_LIST, "validator_list").
-define(S_VALIDATOR_ELECTED_LIST, "validator_elected_list").
-define(S_VALIDATOR_ELECTION_LIST, "validator_election_list").
-define(S_OWNER_VALIDATOR_LIST_BEFORE, "owner_validator_list_before").
-define(S_OWNER_VALIDATOR_LIST, "owner_validator_list").
-define(S_VALIDATOR, "validator").
-define(VALIDATOR_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    ValidatorListLimit = "limit " ++ integer_to_list(?VALIDATOR_LIST_LIMIT),
    Loads = [
        {?S_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {scope, validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR_LIST,
            {validator_list_base, [
                {scope, ""},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {scope, owner_validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST,
            {validator_list_base, [
                {scope, owner_validator_list_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR_ELECTED_LIST,
            {validator_elected_list, [
                {filter, "and block <= $1"},
                {validator_select,
                    {validator_list_base, [
                        {scope, validator_elected_list_scope},
                        {order, ""},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_VALIDATOR_ELECTION_LIST,
            {validator_elected_list, [
                {filter, "and hash = $1"},
                {validator_select,
                    {validator_list_base, [
                        {scope, validator_elected_list_scope},
                        {order, ""},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_VALIDATOR,
            {validator_list_base, [
                {scope, "where l.address = $1"},
                {order, ""},
                {limit, ""}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "validators.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_validator_list([{owner, undefined} | Args]), block_time);
handle('GET', [<<"elected">>], _Req) ->
    {ok, #{height := Height}} = bh_route_blocks:get_block_height(),
    ?MK_RESPONSE(get_validator_elected_list({height, Height}), block_time);
handle('GET', [<<"elected">>, BlockId], _Req) ->
    try binary_to_integer(BlockId) of
        Height -> ?MK_RESPONSE(get_validator_elected_list({height, Height}), infinity)
    catch
        _:_ ->
            ?RESPONSE_400
    end;
handle('GET', [<<"elected">>, <<"hash">>, TxnHash], _Req) ->
    ?MK_RESPONSE(get_validator_elected_list({hash, TxnHash}), infinity);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_validator(Address), never);
handle(_, _, _Req) ->
    ?RESPONSE_404.

get_validator_list([{owner, undefined}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_VALIDATOR_LIST, []),
    mk_validator_list_from_result(Result);
get_validator_list([{owner, Owner}, {cursor, undefined}]) ->
    Result = ?PREPARED_QUERY(?S_OWNER_VALIDATOR_LIST, [Owner]),
    mk_validator_list_from_result(Result);
get_validator_list([{owner, Owner}, {cursor, Cursor}]) ->
    case ?CURSOR_DECODE(Cursor) of
        {ok, #{
            <<"before_address">> := BeforeAddress,
            <<"before_block">> := BeforeBlock,
            <<"height">> := _Height
        }} ->
            case Owner of
                undefined ->
                    Result =
                        ?PREPARED_QUERY(?S_VALIDATOR_LIST_BEFORE, [
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_validator_list_from_result(Result);
                Owner ->
                    Result =
                        ?PREPARED_QUERY(?S_OWNER_VALIDATOR_LIST_BEFORE, [
                            Owner,
                            BeforeAddress,
                            BeforeBlock
                        ]),
                    mk_validator_list_from_result(Result)
            end;
        _ ->
            {error, badarg}
    end.

get_validator_elected_list({height, Height}) ->
    Result = ?PREPARED_QUERY(?S_VALIDATOR_ELECTED_LIST, [Height]),
    mk_validator_list_from_result(Result);
get_validator_elected_list({hash, TxnHash}) ->
    Result = ?PREPARED_QUERY(?S_VALIDATOR_ELECTION_LIST, [TxnHash]),
    mk_validator_list_from_result(Result).

get_validator(Address) ->
    case ?PREPARED_QUERY(?S_VALIDATOR, [Address]) of
        {ok, _, [Result]} ->
            {ok, validator_to_json(Result)};
        _ ->
            {error, not_found}
    end.

mk_validator_list_from_result({ok, _, Results}) ->
    {ok, validator_list_to_json(Results), mk_cursor(Results)}.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?VALIDATOR_LIST_LIMIT of
        true ->
            undefined;
        false ->
            {Height, Address, _Owner, _Stake, _Status, _LastHeartbeat, _VersionHeartBeat, _Nonce,
                FirstBlock, _OnlineStatus, _BlockStatus, _ListenAddrs} = lists:last(Results),
            #{
                before_address => Address,
                before_block => FirstBlock,
                height => Height
            }
    end.

%%
%% json
%%

validator_list_to_json(Results) ->
    lists:map(fun validator_to_json/1, Results).

validator_to_json(
    {Height, Address, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, _Nonce, _FirstBlock,
        OnlineStatus, BlockStatus, ListenAddrs}
) ->
    %% Excluded nonce for now as it is unused
    #{
        <<"address">> => Address,
        <<"owner">> => Owner,
        <<"stake">> => Stake,
        <<"status">> => Status,
        <<"last_heartbeat">> => LastHeartbeat,
        <<"version_heartbeat">> => VersionHeartbeat,
        <<"block">> => Height,
        status => #{
            online => OnlineStatus,
            height => BlockStatus,
            listen_addrs => ListenAddrs
        }
    }.
