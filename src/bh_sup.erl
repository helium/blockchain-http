-module(bh_sup).

-behaviour(supervisor).
-include("bh_db_worker.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-define(WORKER(I, Args), #{
    id => I,
    start => {I, start_link, Args},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [I]
}).


start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 10,
                 period => 10},
    {ok, DBOpts} = psql_migration:connection_opts([]),
    {ok, DBHandlers} = application:get_env(blockchain_http, db_handlers),
    {ok, PoolArgs} = application:get_env(blockchain_http, db_pool),
    {ok, ListenPort} = application:get_env(blockchain_http, port),
    lager:info("Starting http listener on ~p", [ListenPort]),
    ChildSpecs =
        [
         poolboy:child_spec(?DB_POOL,
                            [
                             {name, {local, ?DB_POOL}},
                             {worker_module, bh_db_worker}
                             ] ++ PoolArgs,
                            [
                             {db_opts, DBOpts},
                             {db_handlers, DBHandlers}
                            ]),
         ?WORKER(elli, [
                        [
                         {callback, bh_routes},
                         {port, ListenPort}
                        ]
                       ])
        ],

    {ok, {SupFlags, ChildSpecs}}.
