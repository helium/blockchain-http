-module(bh_db_worker).

-include("bh_route_handler.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-callback prepare_conn(epgsql:connection()) -> map().

-behaviour(dispcount).

%% how long the pool worker, if we get one, has to give us the db conn
-define(POOL_CHECKOUT_TIMEOUT, 500).
%% how many times to try to get a worker
-define(POOL_CHECKOUT_RETRIES, 3).

-export([init/1, checkout/2, transaction/3, checkin/2, handle_info/2, dead/1,
         terminate/2, code_change/3]).

-export([squery/2, equery/3, prepared_query/3]).

-record(state,
        {
         given = false :: boolean(),
         db_conn :: epgsql:connection(),
         handlers :: [atom()],
         prepared_statements :: map()
        }).


-spec squery(Pool::term(), Stmt::string()) -> epgsql_cmd_squery:response().
squery(shutdown, _) ->
    throw(?RESPONSE_503_SHUTDOWN);
squery(Pool, Sql) ->
    case do_checkout(Pool, ?POOL_CHECKOUT_RETRIES) of
        {ok, Reference, Conn} ->
            Ref = epgsqla:squery(Conn, Sql),
            dispcount:checkin(Pool, Reference, Conn),
            receive
                {Conn, Ref, Res} ->
                    Res
            after
                500 ->
                    throw(?RESPONSE_503)
            end;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

-spec equery(Pool::term(), Stmt::string(), Params::[epgsql:bind_param()]) -> epgsql_cmd_equery:response().
equery(shutdown, _, _) ->
    throw(?RESPONSE_503_SHUTDOWN);
equery(Pool, Stmt, Params) ->
    case do_checkout(Pool, ?POOL_CHECKOUT_RETRIES) of
        {ok, Reference, Conn} ->
            case epgsql:parse(Conn, Stmt) of
                {ok, #statement{types = Types} = Statement} ->
                    TypedParameters = lists:zip(Types, Params),
                    Ref = epgsqla:equery(Conn, Statement, TypedParameters),
                    dispcount:checkin(Pool, Reference, Conn),
                    receive
                        {Conn, Ref, Res} ->
                            Res
                    after
                        500 ->
                            throw(?RESPONSE_503)
                    end;
                Error ->
                    dispcount:checkin(Pool, Reference, Conn),
                    lager:warning("failed to parse query ~p : ~p", [Stmt, Error]),
                    throw({500, [], <<"Internal server error">>})
            end;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

-spec prepared_query(Pool::term(), Name::string(), Params::[epgsql:bind_param()]) -> epgsql_cmd_prepared_query:response().
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
                  gen_server:cast(Conn, {{cast, From, Ref}, epgsql_cmd_prepared_query, {Statement, TypedParameters}})
          end,
    case dispcount:transaction(Pool, Fun) of
        ok ->
            receive
                {_Conn, Ref, Res} ->
                    Res
            after
                500 ->
                    throw(?RESPONSE_503)
            end;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

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
    PreparedStatements = lists:foldl(fun(Mod, Acc) ->
                                             maps:merge(Mod:prepare_conn(Conn), Acc)
                                     end, #{}, Handlers),
    {ok, #state{db_conn=Conn, given=false, handlers=Handlers, prepared_statements=PreparedStatements}}.

checkout(_From, State = #state{given=true}) ->
    lager:warning("unexpected checkout when already checked out"),
    {error, busy, State};
checkout(_From, State = #state{db_conn = Conn}) ->
    {ok, Conn, State#state{given=true}}.

transaction(From, Fun, State = #state{db_conn=Conn, prepared_statements=Stmts}) ->
    Fun(From, {Stmts, Conn}),
    {ok, State}.

checkin(Conn, State = #state{db_conn = Conn, given=true}) ->
    {ok, State#state{given=false}};
checkin(Conn, State) ->
    lager:warning("unexpected checkin of ~p when we have ~p", [Conn, State#state.db_conn]),
    {ignore, State}.

dead(State) ->
    {ok, State#state{given=false}}.

handle_info({'EXIT', Conn, Reason}, State = #state{db_conn=Conn}) ->
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

do_checkout(_Pool, 0) ->
    {error, busy};
do_checkout(Pool, Tries) ->
    case dispcount:checkout(Pool, ?POOL_CHECKOUT_TIMEOUT) of
        {error, busy} ->
            do_checkout(Pool, Tries - 1);
        Res -> Res
    end.
