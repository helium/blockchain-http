-module(bh_sup).

-behaviour(supervisor).
-include("bh_db_worker.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-define(WORKER(I, Args),
        #{
          id => I,
          start => {I, start_link, Args},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [I]
         }).

-define(DB_WORKER(N, A, O),
        poolboy:child_spec(N,
                            [
                             {name, {local, N}},
                             {worker_module, bh_db_worker}
                             ] ++ (A), (O) )
       ).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 10,
                 period => 10},
    {ok, RW_PoolOpts} = application:get_env(blockchain_http, db_rw_pool),
    {ok, RW_DBOpts} = psql_migration:connection_opts([], "DATABASE_RW_URL"),
    {ok, RW_DBHandlers} = application:get_env(blockchain_http, db_rw_handlers),

    {ok, RO_PoolOpts} = application:get_env(blockchain_http, db_ro_pool),
    {ok, RO_DBOpts} = psql_migration:connection_opts([], "DATABASE_RO_URL"),
    {ok, RO_DBHandlers} = application:get_env(blockchain_http, db_ro_handlers),

    {ok, ListenPort} = application:get_env(blockchain_http, port),
    lager:info("Starting http listener on ~p", [ListenPort]),
    ChildSpecs =
        [
         ?DB_WORKER(?DB_RO_POOL, RO_PoolOpts, [{db_opts, RO_DBOpts}, {db_handlers, RO_DBHandlers}]),
         ?DB_WORKER(?DB_RW_POOL, RW_PoolOpts, [{db_opts, RW_DBOpts}, {db_handlers, RW_DBHandlers}]),
         ?WORKER(elli, [ [{callback, bh_routes}, {port, ListenPort}] ])
        ],

    {ok, {SupFlags, ChildSpecs}}.
