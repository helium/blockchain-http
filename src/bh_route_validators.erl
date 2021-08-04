-module(bh_route_validators).

-behavior(bh_route_handler).
-behavior(bh_db_worker).

-include("bh_route_handler.hrl").

-export([prepare_conn/1, handle/3]).
%% Utilities
-export([get_validator_list/1, get_validator/1]).
%% Stats
-export([get_validator_stats/0]).

-define(S_VALIDATOR_LIST_BEFORE, "validator_list_before").
-define(S_VALIDATOR_LIST, "validator_list").
-define(S_VALIDATOR_ELECTED_LIST, "validator_elected_list").
-define(S_VALIDATOR_ELECTION_LIST, "validator_election_list").
-define(S_OWNER_VALIDATOR_LIST_BEFORE, "owner_validator_list_before").
-define(S_OWNER_VALIDATOR_LIST, "owner_validator_list").
-define(S_VALIDATOR, "validator").
-define(S_VALIDATORS_NAMED, "validator_named").
-define(S_VALIDATOR_NAME_SEARCH, "validators_name_search").
-define(S_VALIDATOR_STATS, "validators_stats").
-define(S_ACTIVE_VALIDATORS, "active_validators").

-define(VALIDATOR_LIST_LIMIT, 100).
-define(VALIDATOR_LIST_NAME_SEARCH_LIMIT, 100).

prepare_conn(Conn) ->
    ValidatorListLimit = "limit " ++ integer_to_list(?VALIDATOR_LIST_LIMIT),
    ValidatorListNameSearchLimit = "limit " ++ integer_to_list(?VALIDATOR_LIST_NAME_SEARCH_LIMIT),

    Loads = [
        {?S_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {source, ""},
                {scope, validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR_LIST,
            {validator_list_base, [
                {source, ""},
                {scope, ""},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST_BEFORE,
            {validator_list_base, [
                {source, ""},
                {scope, owner_validator_list_before_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_OWNER_VALIDATOR_LIST,
            {validator_list_base, [
                {source, ""},
                {scope, owner_validator_list_scope},
                {order, validator_list_order},
                {limit, ValidatorListLimit}
            ]}},
        {?S_VALIDATOR_ELECTED_LIST,
            {validator_elected_list, [
                {filter, "and block <= $1"},
                {validator_select,
                    {validator_list_base, [
                        {source, ""},
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
                        {source, ""},
                        {scope, validator_elected_list_scope},
                        {order, ""},
                        {limit, ""}
                    ]}}
            ]}},
        {?S_VALIDATOR,
            {validator_list_base, [
                {source, validator_source},
                {scope, "where l.address = $1"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_VALIDATORS_NAMED,
            {validator_list_base, [
                {source, ""},
                {scope, "where l.name = $1"},
                {order, ""},
                {limit, ""}
            ]}},
        {?S_VALIDATOR_NAME_SEARCH,
            {validator_list_base, [
                {source, ""},
                {scope, validator_name_search_scope},
                {order, ""},
                {limit, ValidatorListNameSearchLimit}
            ]}},
        {?S_VALIDATOR_STATS, {validator_stats, []}},
        {?S_ACTIVE_VALIDATORS, {validator_active, []}}
    ],
    bh_db_worker:load_from_eql(Conn, "validators.sql", Loads).

handle('GET', [], Req) ->
    Args = ?GET_ARGS([cursor], Req),
    ?MK_RESPONSE(get_validator_list([{owner, undefined} | Args]), block_time);
handle('GET', [<<"stats">>], _Req) ->
    ?MK_RESPONSE(get_stats(), block_time);
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
handle('GET', [<<"name">>], Req) ->
    Args = ?GET_ARGS([search], Req),
    ?MK_RESPONSE(get_validator_list(Args), block_time);
handle('GET', [<<"name">>, Name], _Req) ->
    ?MK_RESPONSE(get_validators_named(Name), block_time);
handle('GET', [Address], _Req) ->
    ?MK_RESPONSE(get_validator(Address), never);
handle('GET', [Address, <<"activity">>], Req) ->
    Args = ?GET_ARGS([cursor, filter_types], Req),
    Result = bh_route_txns:get_activity_list({validator, Address}, Args),
    CacheTime = bh_route_txns:get_txn_list_cache_time(Result),
    ?MK_RESPONSE(Result, CacheTime);
handle('GET', [Address, <<"activity">>, <<"count">>], Req) ->
    Args = ?GET_ARGS([filter_types], Req),
    ?MK_RESPONSE(bh_route_txns:get_activity_count({validator, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>], Req) ->
    Args = ?GET_ARGS([cursor, max_time, min_time], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_list({validator, Address}, Args), block_time);
handle('GET', [Address, <<"rewards">>, <<"sum">>], Req) ->
    Args = ?GET_ARGS([max_time, min_time, bucket], Req),
    ?MK_RESPONSE(bh_route_rewards:get_reward_sum({validator, Address}, Args), block_time);
handle('GET', [<<"rewards">>, <<"sum">>], Req) ->
    %% We do not allow bucketing across all validators as that takes way too long
    Args = ?GET_ARGS([max_time, min_time], Req),
    ?MK_RESPONSE(
        bh_route_rewards:get_reward_sum({validator, all}, Args ++ [{bucket, undefined}]),
        block_time
    );
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
    end;
get_validator_list([{search, Name}]) ->
    Result = ?PREPARED_QUERY(?S_VALIDATOR_NAME_SEARCH, [Name]),
    mk_validator_list_from_result(Result).

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

get_validators_named(Name) ->
    case ?PREPARED_QUERY(?S_VALIDATORS_NAMED, [Name]) of
        {ok, _, Results} ->
            {ok, validator_list_to_json(Results)};
        _ ->
            {error, not_found}
    end.
get_validator_stats() ->
    bh_cache:get({?MODULE, block_stats}, fun() ->
        {ok, _Columns, Data} = ?PREPARED_QUERY(?S_VALIDATOR_STATS, []),
        Data
    end).

get_stats() ->
    [
        ActiveStats,
        ValidatorStats
    ] = ?EXECUTE_BATCH([
        {?S_ACTIVE_VALIDATORS, []},
        {?S_VALIDATOR_STATS, []}
    ]),
    Stats = mk_stats_from_validator_results(ValidatorStats),
    %% Inject active validators in the result
    Active =
        case ActiveStats of
            {ok, []} -> null;
            {ok, [{_, C}]} -> C
        end,
    {ok, Stats#{active => Active}}.

mk_validator_list_from_result({ok, _, Results}) ->
    {ok, validator_list_to_json(Results), mk_cursor(Results)}.

mk_cursor(Results) when is_list(Results) ->
    case length(Results) < ?VALIDATOR_LIST_LIMIT of
        true ->
            undefined;
        false ->
            case lists:last(Results) of
                {Height, Address, _Name, _Owner, _Stake, _Status, _LastHeartbeat, _VersionHeartBeat,
                    _Penalty, _Penalties, _Nonce, FirstBlock, _OnlineStatus, _BlockStatus,
                    _StatusTimestamp, _ListenAddrs} ->
                    #{
                        before_address => Address,
                        before_block => FirstBlock,
                        height => Height
                    };
                {Height, Address, _Name, _Owner, _Stake, _Status, _LastHeartbeat, _VersionHeartBeat,
                    _Penalty, _Penalties, _Nonce, FirstBlock, _OnlineStatus, _BlockStatus,
                    _StatusTimestamp, _ListenAddrs, _CGCount} ->
                    #{
                        before_address => Address,
                        before_block => FirstBlock,
                        height => Height
                    }
            end
    end.

%%
%% json
%%

validator_list_to_json(Results) ->
    lists:map(fun validator_to_json/1, Results).

validator_to_json(
    {Height, Address, Name, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Penalty,
        Penalties, _Nonce, FirstBlock, OnlineStatus, BlockStatus, StatusTimestamp, ListenAddrs,
        CGCount}
) ->
    Json = validator_to_json(
        {Height, Address, Name, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Penalty,
            Penalties, _Nonce, FirstBlock, OnlineStatus, BlockStatus, StatusTimestamp, ListenAddrs}
    ),
    Json#{consensus_groups => CGCount};
validator_to_json(
    {Height, Address, Name, Owner, Stake, Status, LastHeartbeat, VersionHeartbeat, Penalty,
        Penalties, _Nonce, FirstBlock, OnlineStatus, BlockStatus, StatusTimestamp, ListenAddrs}
) ->
    MaybeTimestamp = fun
        (null) -> null;
        (V) -> iso8601:format(V)
    end,
    %% Excluded nonce for now as it is unused
    #{
        address => Address,
        name => Name,
        owner => Owner,
        stake => Stake,
        stake_status => Status,
        last_heartbeat => LastHeartbeat,
        version_heartbeat => VersionHeartbeat,
        penalty => Penalty,
        penalties => Penalties,
        block_added => FirstBlock,
        block => Height,
        status => #{
            online => OnlineStatus,
            height => BlockStatus,
            timestamp => MaybeTimestamp(StatusTimestamp),
            listen_addrs => ListenAddrs
        }
    }.

mk_stats_from_validator_results({ok, Results}) ->
    %% We do all this to ensure that various status entries are always
    %% present in the resulting map even if they are not in the sql results
    lists:foldl(
        fun(Key, Acc = #{}) ->
            {Count, Amount} =
                case lists:keyfind(Key, 1, Results) of
                    false -> {0, 0};
                    {_, C, A} -> {C, A}
                end,
            maps:put(Key, #{count => Count, amount => Amount}, Acc)
        end,
        #{},
        [<<"staked">>, <<"unstaked">>, <<"cooldown">>]
    ).
