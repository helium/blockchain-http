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
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 10
    },

    PoolNames =
        case os:getenv("DATABASE_RW_URL") of
            false -> [ro_pool];
            _ -> [ro_pool, rw_pool]
        end,

    lists:foreach(
        fun(PoolName) ->
            {ok, {PoolOpts, DBOpts, DBHandlers}} = pool_opts(PoolName),
            init_pool(PoolName, PoolOpts, DBOpts, DBHandlers)
        end,
        PoolNames
    ),

    {ok, ListenPort} = application:get_env(blockchain_http, port),

    lager:info("Starting http listener on ~p", [ListenPort]),

    ElliConfig = [
        {mods, [
            {bh_middleware_cors, []},
            {bh_routes, []}
        ]}
    ],
    ChildSpecs =
        [
            ?WORKER(bh_cache, []),
            ?WORKER(bh_pool_watcher, [PoolNames]),
            ?WORKER(elli, [
                [
                    {callback, elli_middleware},
                    {callback_args, ElliConfig},
                    {port, ListenPort}
                ]
            ])
        ],

    {ok, {SupFlags, ChildSpecs}}.

pool_opts(ro_pool) ->
    {ok, DBOpts} = psql_migration:connection_opts([], {env, "DATABASE_RO_URL"}),
    {ok, DBHandlers} = application:get_env(blockchain_http, db_ro_handlers),
    {ok, PoolOpts0} = application:get_env(blockchain_http, db_ro_pool),
    PoolSize =
        case os:getenv("DATABASE_RO_POOL_SIZE") of
            false -> proplists:get_value(size, PoolOpts0, 100);
            SizeStr -> list_to_integer(SizeStr)
        end,
    PoolOpts = lists:keystore(size, 1, PoolOpts0, {size, PoolSize}),
    {ok, {PoolOpts, DBOpts, DBHandlers}};
pool_opts(rw_pool) ->
    {ok, PoolOpts} = application:get_env(blockchain_http, db_rw_pool),
    {ok, DBOpts} = psql_migration:connection_opts([], {env, "DATABASE_RW_URL"}),
    {ok, DBHandlers} = application:get_env(blockchain_http, db_rw_handlers),
    {ok, {PoolOpts, DBOpts, DBHandlers}}.

init_pool(Name, PoolOpts, DBOpts, DBHandlers) ->
    ok = dispcount:start_dispatch(
        Name,
        {bh_db_worker, [
            {db_opts, DBOpts},
            {db_handlers, DBHandlers}
        ]},
        [
            {restart, permanent},
            {shutdown, 4000},
            {dispatch_mechanism, proplists:get_value(dispatch_mechanism, PoolOpts, hash)},
            {watcher_type, proplists:get_value(watcher_type, PoolOpts, ets)},
            {maxr, 10},
            {maxt, 60},
            {resources, proplists:get_value(size, PoolOpts, 5)}
        ]
    ),

    {ok, PoolInfo} = dispcount:dispatcher_info(Name),
    persistent_term:put(Name, PoolInfo),
    ok.
