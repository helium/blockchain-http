-module(bh_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(DB_POOL, db_pool).

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
    ChildSpecs =
        [
         ?WORKER(elli, [
                        [{callback, bh_routes}, {port, 8080}]
                       ]),
         poolboy:child_spec(?DB_POOL,
                            [{name, {local, ?DB_POOL}},
                             {worker_module, bh_db_worker},
                             {size, 10},
                             {max_overflow, 20}
                            ],
                            maps:to_list(DBOpts)
                            )
        ],

    {ok, {SupFlags, ChildSpecs}}.
