-module(bh_db_worker).

-include("bh_route_handler.hrl").

-include_lib("epgsql/include/epgsql.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-callback prepare_conn(epgsql:connection()) -> map().

-behaviour(dispcount).

%% how long the pool worker, if we get one, has to give us the db conn
-define(POOL_CHECKOUT_TIMEOUT, 500).
%% how many times to try to get a worker
-define(POOL_CHECKOUT_RETRIES, 3).
%% how long to wait for a query response
-define(POOL_QUERY_TIMEOUT, 15000).

-export([
    init/1,
    checkout/2,
    transaction/3,
    checkin/2,
    handle_info/2,
    dead/1,
    terminate/2,
    code_change/3
]).

-export([load_from_eql/2]).
-export([prepared_query/3, execute_batch/2]).

-record(state, {
    given = false :: boolean(),
    db_conn :: epgsql:connection(),
    handlers :: [atom()],
    prepared_statements :: map()
}).

-spec prepared_query(Pool :: term(), Name :: string(), Params :: [epgsql:bind_param()]) ->
    epgsql_cmd_prepared_query:response().
prepared_query(shutdown, _, _) ->
    throw(?RESPONSE_503_SHUTDOWN);
prepared_query(Pool, Name, Params) ->
    Ref = make_ref(),
    Fun = fun(From, {Stmts, Conn}) ->
        Statement = maps:get(Name, Stmts),
        #statement{types = Types} = Statement,
        TypedParameters = lists:zip(Types, Params),
        %% construct the same kind of cast the epgsqla:prepared_statement does, but redirect
        %% the output to the elli process directly
        gen_server:cast(
            Conn,
            {{cast, From, Ref}, epgsql_cmd_prepared_query, {Statement, TypedParameters}}
        )
    end,
    case dispcount:transaction(Pool, Fun) of
        ok ->
            receive
                {_Conn, Ref, Res} ->
                    Res
            after ?POOL_QUERY_TIMEOUT -> throw(?RESPONSE_503)
            end;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

-spec execute_batch(Pool :: term(), [{Name :: string(), Params :: [epgsql:bind_param()]}]) ->
    epgsql_cmd_batch:response().
execute_batch(shutdown, _) ->
    throw(?RESPONSE_503_SHUTDOWN);
execute_batch(Pool, Queries) ->
    Ref = make_ref(),
    Fun = fun(From, {Stmts, Conn}) ->
        Batch = lists:foldr(
            fun({Name, Params}, Acc) ->
                Statement = maps:get(Name, Stmts),
                #statement{types = Types} = Statement,
                TypedParameters = lists:zip(Types, Params),
                [{Statement, TypedParameters} | Acc]
            end,
            [],
            Queries
        ),
        %% construct the same kind of cast the epgsqla:prepared_statement does, but redirect
        %% the output to the elli process directly
        gen_server:cast(
            Conn,
            {{cast, From, Ref}, epgsql_cmd_batch, Batch}
        )
    end,
    case dispcount:transaction(Pool, Fun) of
        ok ->
            receive
                {_Conn, Ref, Res} ->
                    Res
            after ?POOL_QUERY_TIMEOUT -> throw(?RESPONSE_503)
            end;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

load_from_eql(Conn, Filename) ->
    PrivDir = code:priv_dir(blockchain_http),
    {ok, Queries} = eql:compile(filename:join(PrivDir, Filename)),
    Statements = lists:map(
        fun({Name, Query}) ->
            Key = atom_to_list(Name),
            {ok, Statement} = epgsql:parse(Conn, Key, Query, []),
            {Key, Statement}
        end,
        Queries
    ),
    maps:from_list(Statements).

init(Args) ->
    GetOpt = fun(K) ->
        case lists:keyfind(K, 1, Args) of
            false -> error({missing_opt, K});
            {_, V} -> V
        end
    end,
    DBOpts = GetOpt(db_opts),
    Codecs = [{epgsql_codec_json, {jiffy, [], [return_maps]}}],
    {ok, Conn} = epgsql:connect(DBOpts#{codecs => Codecs}),
    Handlers = GetOpt(db_handlers),
    PreparedStatements = lists:foldl(
        fun(Mod, Acc) ->
            maps:merge(Mod:prepare_conn(Conn), Acc)
        end,
        #{},
        Handlers
    ),
    {ok, #state{
        db_conn = Conn,
        given = false,
        handlers = Handlers,
        prepared_statements = PreparedStatements
    }}.

checkout(_From, State = #state{given = true}) ->
    lager:warning("unexpected checkout when already checked out"),
    {error, busy, State};
checkout(_From, State = #state{db_conn = Conn}) ->
    {ok, Conn, State#state{given = true}}.

transaction(From, Fun, State = #state{db_conn = Conn, prepared_statements = Stmts}) ->
    try Fun(From, {Stmts, Conn}) of
        _ -> ok
    catch
        What:Why:Stack ->
            lager:warning("Transaction failed: ~p", [{What, Why, Stack}])
    end,
    {ok, State}.

checkin(Conn, State = #state{db_conn = Conn, given = true}) ->
    {ok, State#state{given = false}};
checkin(Conn, State) ->
    lager:warning("unexpected checkin of ~p when we have ~p", [Conn, State#state.db_conn]),
    {ignore, State}.

dead(State) ->
    {ok, State#state{given = false}}.

handle_info({'EXIT', Conn, Reason}, State = #state{db_conn = Conn}) ->
    lager:info("dispcount worker's db connection exited ~p", [Reason]),
    {stop, Reason, State};
handle_info(_Msg, State) ->
    lager:info("dispcount worker got unexpected message ~p ~p", [_Msg, State]),
    {ok, State}.

terminate(_Reason, _State) ->
    %% let the GC clean the socket.
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-ifdef(TEST).
eql_test() ->
    meck:new(epgsql),
    meck:expect(epgsql, parse, fun(fakeconnection, _Key, Query, _Opts) -> {ok, Query} end),
    %% we need to load the application here so that code:priv/2 will work correctly
    ok = application:load(blockchain_http),
    Files = ["stats.sql", "vars.sql"],
    lists:all(fun(#{}) -> true;
                 (_) -> false
              end, [ load_from_eql(fakeconnection, F) || F <- Files ]),
    meck:unload(epgsql).

-endif.
