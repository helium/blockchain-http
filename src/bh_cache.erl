%% @doc In memory cache for expensive query results
-module(bh_cache).

-behaviour(gen_server).

-define(TBL_NAME, '__bh_cache_table').
-define(ETS_OPTS, [named_table, {keypos, 2}, {read_concurrency, true}]).
% seconds
-define(DEFAULT_TTL, 60).
% milliseconds
-define(TICK_INTERVAL, 15000).
% 1000 microseconds to 1 millisecond
-define(TO_MILLIS, 1000).

%% public API
-export([
    start_link/0,
    get/1,
    get/2,
    get/3,
    put/2,
    put/3
]).

%% required callbacks
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2
]).

-record(state, {
    tid = undefined :: ets:tid(),
    tref = undefined :: reference(),
    hits = 0 :: non_neg_integer(),
    misses = [] :: [non_neg_integer()]
    % input is microsecs,
    % but list is milliseconds
}).

-record(entry, {
    key = undefined :: term(),
    value = undefined :: term(),
    expire_ts = 0 :: non_neg_integer()
}).

-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Error :: term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get(Key :: term()) -> not_found | {ok, Value :: term()}.
%% @doc Attempt to lookup a value associated with the given key
%% in the cache. If the key is not found, return `not_found'.
get(Key) ->
    case ets:lookup(?TBL_NAME, Key) of
        [] ->
            not_found;
        [Entry] ->
            maybe_expired(Entry#entry.value, erlang:system_time(seconds), Entry#entry.expire_ts)
    end.

-spec get(
    Key :: term(),
    LookupFun :: fun(() -> Value :: term())
) -> {ok, Value :: term()}.
%% @doc Attempt to lookup a value associated with the given key
%% in the cache. If the key is not found, execute the provided
%% zero arity fun to return a value to cache.
%%
%% This call is equivalent to `get/3' with all options defaulted.
get(Key, LookupFun) ->
    get(Key, LookupFun, #{}).

-spec get(
    Key :: term(),
    LookupFun :: fun(() -> Value :: term()),
    Options :: map()
) -> {ok, Value :: term()}.
%% @doc Attempt to lookup a value associated with the given key
%% in the cache. If the key is not found, execute the provided
%% zero arity fun to return a value to cache.
%%
%% The only defined option at this time is `ttl' which
%% defines the number of seconds to keep a value in the cache.
%% The default time is 60 seconds.
get(Key, LookupFun, Opts) ->
    case ?MODULE:get(Key) of
        not_found ->
            {MicroSecs, Value} = timer:tc(fun() -> LookupFun() end),
            log_miss(Key, MicroSecs),
            ?MODULE:put(Key, Value, Opts);
        V ->
            log_hit(Key),
            V
    end.

-spec put(
    Key :: term(),
    Value :: term()
) -> {ok, Value :: term()}.
%% @doc Associate the given key with the given value.
%%
%% Equivalent to `put/3' with all options defaulted.
put(Key, Value) ->
    put(Key, Value, #{}).

-spec put(
    Key :: term(),
    Value :: term(),
    Options :: map()
) -> {ok, Value :: term()}.
%% @doc Associate the given key with the given value.
%%
%% The only defined option at this time is `ttl' which
%% defines the number of seconds to keep a value in the cache.
%% The default time is 60 seconds.
put(Key, Value, Opts) ->
    gen_server:call(?MODULE, {put, Key, Value, Opts}).

-spec log_hit(Key :: term()) -> ok.
%% @doc (Asynchronously) record a cache hit for the given key
log_hit(Key) ->
    gen_server:cast(?MODULE, {hit, Key}).

-spec log_miss(
    Key :: term(),
    MicroSecs :: non_neg_integer()
) -> ok.
%% @doc (Asynchronously) record a cache miss for the given key;
%% record the amount of computation time to generate the cached
%% answer
log_miss(Key, MicroSecs) ->
    gen_server:cast(?MODULE, {miss, Key, MicroSecs}).

%% gen server callbacks
init([]) ->
    Tid = ets:new(?TBL_NAME, ?ETS_OPTS),
    Tref = schedule_new_tick(),
    {ok, #state{tid = Tid, tref = Tref}}.

handle_call({put, Key, Value, Opts}, _From, State) ->
    TTL = maps:get(ttl, Opts, ?DEFAULT_TTL),
    ExpireTime = erlang:system_time(seconds) + TTL,
    true = ets:insert(
        ?TBL_NAME,
        #entry{
            key = Key,
            value = Value,
            expire_ts = ExpireTime
        }
    ),
    {reply, {ok, Value}, State};
handle_call(Call, From, State) ->
    lager:warning("Unexpected call ~p from ~p", [Call, From]),
    {reply, diediedie, State}.

handle_cast({hit, _Key}, #state{hits = H} = State) ->
    {noreply, State#state{hits = H + 1}};
handle_cast({miss, _Key, Micros}, #state{misses = M} = State) ->
    {noreply, State#state{misses = [Micros div ?TO_MILLIS | M]}};
handle_cast(Cast, State) ->
    lager:warning("Unexpected cast ~p", [Cast]),
    {noreply, State}.

handle_info(bh_cache_tick, State) ->
    ok = expire_cache(),
    NewState = compute_cache_stats(State),
    Tref = schedule_new_tick(),
    {noreply, NewState#state{tref = Tref}};
handle_info(Info, State) ->
    lager:warning("Unexpected info ~p", [Info]),
    {noreply, State}.

%% internal functions
-spec maybe_expired(
    Value :: term(),
    Current :: non_neg_integer(),
    Expire :: non_neg_integer()
) -> not_found | {ok, Value :: term()}.
maybe_expired(_Value, Current, Expire) when Current >= Expire -> not_found;
maybe_expired(Value, _Current, _Expire) -> {ok, Value}.

-spec schedule_new_tick() -> reference().
schedule_new_tick() ->
    erlang:send_after(?TICK_INTERVAL, self(), bh_cache_tick).

-spec expire_cache() -> ok.
expire_cache() ->
    Current = erlang:system_time(seconds),
    Removed = ets:foldl(
        fun
            (#entry{key = K, expire_ts = E}, Acc) when Current >= E ->
                true = ets:delete(?TBL_NAME, K),
                Acc + 1;
            (_Entry, Acc) ->
                Acc
        end,
        0,
        ?TBL_NAME
    ),
    case Removed of
        0 ->
            ok;
        _ ->
            lager:info("Removed ~p cache entries this tick.", [Removed]),
            ok
    end.

compute_cache_stats(#state{hits = 0, misses = []} = State) ->
    State;
compute_cache_stats(#state{hits = H, misses = []} = State) ->
    lager:info("Cache hits: ~p", [H]),
    State#state{hits = 0};
compute_cache_stats(#state{hits = H, misses = M} = State) ->
    Misses = length(M),
    Max = lists:max(M),
    Min = lists:min(M),
    Avg = lists:sum(M) div Misses,
    lager:info(
        "Cache hits: ~p, misses count: ~p, max ms: ~p, min ms: ~p, avg ms: ~p",
        [H, Misses, Max, Min, Avg]
    ),
    State#state{hits = 0, misses = []}.
