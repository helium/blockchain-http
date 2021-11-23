-module(bh_pool_watcher).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Pools) ->
    gen_server:start_link(?MODULE, Pools, []).

init(Pools) ->
    Monitors = maps:from_list([
        {erlang:monitor(process, get_name(PoolName)), PoolName}
        || PoolName <- Pools
    ]),
    {ok, Monitors}.

handle_call(Msg, From, State) ->
    lager:warning("Unexpected call ~p from ~p", [Msg, From]),
    {reply, error, State}.

handle_cast(Msg, State) ->
    lager:warning("Unexpected cast ~p", [Msg]),
    {noreply, State}.

handle_info({monitor, Name}, State) ->
    Ref = erlang:monitor(process, get_name(Name)),
    try dispcount:dispatcher_info(Name) of
        {ok, PoolInfo} ->
            lager:info("dispcount is back"),
            %% it's back, update the persistent term
            persistent_term:put(Name, PoolInfo),
            {noreply, maps:put(Ref, Name, State)}
    catch
        What:Why ->
            lager:info("dispcount still not ready ~p:~p", [What, Why]),
            %% likely things have not finished restarting, try again shortly
            erlang:demonitor(Ref, [flush]),
            erlang:send_after(500, self(), {monitor, Name}),
            {noreply, State}
    end;
handle_info({'DOWN', Ref, process, _Pid, noproc}, State) ->
    case maps:find(Ref, State) of
        {ok, Name} ->
            lager:notice("Pool ~p monitor failed with noproc, retrying", [Name]),
            %% noproc means the process wasn't alive when we tried to monitor it
            %% we should probably wait a bit and retry
            erlang:send_after(5000, self(), {monitor, Name}),
            {noreply, maps:remove(Ref, State)};
        error ->
            lager:warning("unknown ref ~p exited with reason noproc", [Ref]),
            {noreply, State}
    end;
handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
    case maps:find(Ref, State) of
        {ok, Name} ->
            self() ! {monitor, Name},
            lager:notice("Pool ~p exited with reason ~p", [Name, Reason]),
            {noreply, maps:remove(Ref, State)};
        error ->
            lager:warning("unknown ref ~p exited with reason ~p", [Ref, Reason]),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    lager:warning("Unexpected info ~p", [Msg]),
    {noreply, State}.

get_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ "_serv").
