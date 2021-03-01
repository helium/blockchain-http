-module(bh_route_validators).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_validator_list/1, get_validator/1]).

-define(S_VALIDATOR_LIST_BEFORE, "validator_list_before").
-define(S_VALIDATOR_LIST, "validator_list").
-define(S_OWNER_VALIDATOR_LIST_BEFORE, "owner_validator_list_before").
-define(S_OWNER_VALIDATOR_LIST, "owner_validator_list").
-define(S_VALIDATOR, "validator").
-define(VALIDATOR_LIST_LIMIT, 100).

prepare_conn(Conn) ->
    ValidatorListLimit = "limit " ++ integer_to_list(?VALIDATOR_LIST_LIMIT),
    Loads = [
        {?S_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {extend, ""},
                {scope, validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR_LIST,
            {validator_list_base, [
                {extend, ""},
                {scope, ""},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {extend, ""},
                {scope, owner_validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST,
            {validator_list_base, [
                {extend, ""},
                {scope, owner_validator_list_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR,
            {validator_list_base, [
                {extend, validator_speculative_extend},
                {scope, "where l.address = $1"},
                {order, ""},
                {limit, ""}
            ]}}
    ],
    bh_db_worker:load_from_eql(Conn, "validators.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_validator_list([{owner, undefined} | Args]), block_time);
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
                FirstBlock} = lists:last(Results),
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
    {Height, Address, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Nonce, _FirstBlock}
) ->
    #{
        <<"address">> => Address,
        <<"owner">> => Owner,
        <<"nonce">> => Nonce,
        <<"stake">> => Stake,
        <<"status">> => Status,
        <<"last_heartbeat">> => LastHeartbeat,
        <<"version_heartbeat">> => VersionHeartbeat,
        <<"block">> => Height
    };
validator_to_json(
    {Height, Address, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Nonce, FirstBlock,
        SpecNonce}
) ->
    Base = validator_to_json(
        {Height, Address, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Nonce, FirstBlock}
    ),
    Base#{
        <<"speculative_nonce">> => SpecNonce
    }.
