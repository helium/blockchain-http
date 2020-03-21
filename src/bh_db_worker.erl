-module(bh_db_worker).

-include("bh_route_handler.hrl").

-callback prepare_conn(epgsql:connection()) -> ok.

-behaviour(dispcount).

%% how long the pool worker, if we get one, has to give us the db conn
-define(POOL_CHECKOUT_TIMEOUT, 500).
%% how many times to try to get a worker
-define(POOL_CHECKOUT_RETRIES, 3).

-export([init/1, checkout/2, checkin/2, handle_info/2, dead/1,
         terminate/2, code_change/3]).

-export([squery/2, equery/3, prepared_query/3]).

-record(state,
        {
         given = false :: boolean(),
         db_conn :: epgsql:connection()
        }).


-spec squery(Pool::term(), Stmt::string()) -> epgsql_cmd_squery:response().
squery(Pool, Sql) ->
    case do_checkout(Pool, ?POOL_CHECKOUT_RETRIES) of
        {ok, Reference, Conn} ->
            Res = epgsql:squery(Conn, Sql),
            dispcount:checkin(Pool, Reference, Conn),
            Res;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

-spec equery(Pool::term(), Stmt::string(), Params::[epgsql:bind_param()]) -> epgsql_cmd_equery:response().
equery(Pool, Stmt, Params) ->
    case do_checkout(Pool, ?POOL_CHECKOUT_RETRIES) of
        {ok, Reference, Conn} ->
            Res = epgsql:equery(Conn, Stmt, Params),
            dispcount:checkin(Pool, Reference, Conn),
            Res;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

-spec prepared_query(Pool::term(), Name::string(), Params::[epgsql:bind_param()]) -> epgsql_cmd_prepared_query:response().
prepared_query(Pool, Name, Params) ->
    case do_checkout(Pool, ?POOL_CHECKOUT_RETRIES) of
        {ok, Reference, Conn} ->
            Res = epgsql:prepared_query(Conn, Name, Params),
            dispcount:checkin(Pool, Reference, Conn),
            Res;
        {error, busy} ->
            throw(?RESPONSE_503)
    end.

init(Args) ->
    process_flag(trap_exit, true),
    GetOpt = fun(K) ->
                     case lists:keyfind(K, 1, Args) of
                         false -> error({missing_opt, K});
                         {_, V} -> V
                     end
             end,
    DBOpts = GetOpt(db_opts),
    Codecs = [{epgsql_codec_json, {jiffy, [], [return_maps]}}],
    {ok, Conn} = epgsql:connect(DBOpts#{codecs => Codecs}),
    lists:foreach(fun(Mod) ->
                          Mod:prepare_conn(Conn)
                  end, GetOpt(db_handlers)),
    {ok, #state{db_conn=Conn}}.

checkout(_From, State = #state{given=true}) ->
    lager:warning("unexpected checkout when already checked out"),
    {error, busy, State};
checkout(_From, State = #state{db_conn = Conn}) ->
    {ok, Conn, State#state{given=true}}.

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
    lager:info("dispcount worker got unexpected message ~p", [_Msg]),
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
