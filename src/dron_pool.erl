-module(dron_pool).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/2, add_worker/1, add_worker/2, add_workers/1,
         auto_add_workers/0, offer_workers/1, take_workers/1, remove_worker/1,
         get_worker/0, release_worker_slot/1, get_all_workers/0,
         master_coordinator/1]).

-record(state, {master_coordinator, worker_policy}).

%===============================================================================

start_link(Master, WorkerPolicy) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [Master, WorkerPolicy], []).

%%------------------------------------------------------------------------------
%% @doc
%% Add a new worker to the pool. The worker will have the number of slots
%% defined in dron_config.
%%
%% @spec add_worker(WorkerName) -> ok | {error, no_connection} | Error
%% @end
%%------------------------------------------------------------------------------
add_worker(WName) ->
    gen_server:call(?MODULE, {add, WName, dron_config:max_slots()}).

%%------------------------------------------------------------------------------
%% @doc
%% Add a new worker to the pool.
%%
%% @spec add_worker(WorkerName, MaxSlots) -> ok | {error, no_connection} | Error
%% @end
%%------------------------------------------------------------------------------
add_worker(WName, MaxSlots) ->
    gen_server:call(?MODULE, {add, WName, MaxSlots}).

add_workers(Workers) ->
    lists:map(fun add_worker/1, Workers).

%%------------------------------------------------------------------------------
%% @doc
%% Adds the workers defined in DRON_WORKERS env variable.
%%
%% @spec auto_add_workers() -> [{WorkerName, Result}]
%% @end
%%------------------------------------------------------------------------------
auto_add_workers() ->
    Workers = dron_config:expand_node_names("DRON_WORKERS"),
    lists:zip(Workers, lists:map(fun add_worker/1, Workers)).

%%------------------------------------------------------------------------------
%% @doc
%% Adds a list of workers to the pool. It does not try to start a dron_worker
%% on the worker nodes.
%%
%% @spec offer_workers(Workers) -> [ok | {error, unknown_worker}]
%% @end
%%------------------------------------------------------------------------------
offer_workers(Workers) ->
    gen_server:call(?MODULE, {offer_workers, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Removes a list of workers from the pool. It does not disable them.
%%
%% @spec take_workers(Workers) -> [ok | {error, unknown_worker}]
%% @end
%%------------------------------------------------------------------------------
take_workers(Workers) ->
    gen_server:call(?MODULE, {take_workers, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Removes a worker from the pool.
%%
%% @spec remove_worker(WorkerName) -> ok | {error, unknown_worker} | Error
%% @end
%%------------------------------------------------------------------------------
remove_worker(WName) ->
    gen_server:call(?MODULE, {remove, WName}).

%%------------------------------------------------------------------------------
%% @doc
%% Get a worker with at least a free slot.
%%
%% @spec get_worker() -> WorkerName
%% @end
%%------------------------------------------------------------------------------
%% TODO(ionel): Check how scheduler's failure can affect the state of slots. I
%% think there may be a leak here if the scheduler fails after it has acquire
%% a slot and before it started running the job instance on it.
get_worker() ->
    gen_server:call(?MODULE, get_worker).

%%------------------------------------------------------------------------------
%% @doc
%% Releases a slot on a given worker.
%%
%% @spec release_worker_slot(WorkerName) -> ok | {error, unknown_worker}
%% @end
%%------------------------------------------------------------------------------
release_worker_slot(WName) ->
    gen_server:call(?MODULE, {release_slot, WName}).

get_all_workers() ->
    gen_server:call(?MODULE, get_all_workers).

%%------------------------------------------------------------------------------
%% @doc
%% Informs the pool about the name of the master coordinator.
%%
%% @spec master_coordinator(MasterName) -> ok
%% @end
%%------------------------------------------------------------------------------
master_coordinator(Master) ->
    gen_server:cast(?MODULE, {master_coordinator, Master}).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([Master, WorkerPolicy]) ->
    ets:new(worker_records, [named_table]),
    ets:new(slot_workers, [ordered_set, named_table]),
    reconstruct_state(),
    erlang:send_after(dron_config:scheduler_heartbeat_interval(),
                      self(), heartbeat),
    {ok, #state{master_coordinator = Master, worker_policy = WorkerPolicy}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({add, WName, Slots}, _From, State) ->
    case ets:lookup(worker_records, WName) of
        [{WName, _}] -> {reply, {error, already_added}, State};
        []           ->
            case net_adm:ping(WName) of
                pong -> WRet = rpc:call(WName, dron_worker, start, [WName]),
                        error_logger:info_msg("Start worker ~p returned ~p",
                                              [WName, WRet]),
                        monitor_node(WName, true),
                        SWorkers = case ets:lookup(slot_workers, 0) of
                                       [{0, CurSws}] -> CurSws;
                                       []            -> []
                                   end,
                        ets:insert(slot_workers, {0, [WName | SWorkers]}),
                        NewW = #worker{name = WName, scheduler = node(),
                                       enabled = true, max_slots = Slots,
                                       used_slots = 0},
                        case dron_db:store_worker(NewW) of
                            ok    -> ets:insert(worker_records, {WName, NewW}),
                                     {reply, ok, State};
                            Error -> {reply, Error, State}
                        end;
                _    -> {reply, {error, no_connection}, State}
            end
    end;
handle_call({offer_workers, Workers}, _From, State) ->
    Result = lists:map(fun(WName) ->
                case dron_db:get_worker(WName) of
                    {ok, Worker = #worker{used_slots = UsedSlots}} ->
                        monitor_node(WName, true),
                        ets:insert(worker_records, {WName, Worker}),
                        SWorkers = case ets:lookup(slot_workers, UsedSlots) of
                                       []     -> [WName];
                                       CurSWs -> [WName | CurSWs]
                                   end,
                        ets:insert(slot_workers, {UsedSlots, SWorkers}),
                        ok;
                    Error -> Error
                end
                       end, Workers),
    {reply, Result, State};
handle_call({take_workers, Workers}, _From, State) ->
    Result = lists:map(fun(WName) ->
                case ets:lookup(worker_records, WName) of
                    [{WName, Worker}] ->
                        % TODO(ionel): Figure out a way of stopp monitoring only
                        % when all the current jobs have finished!
                        monitor_node(WName, false),
                        evict_worker(Worker),
                        ets:delete(worker_records, WName),
                        ok;
                    []                ->
                        {error, unknown_worker}
                end
                       end, Workers),
    {reply, Result, State};
handle_call({remove, WName}, _From, State) ->
    case ets:lookup(worker_records, WName) of
        [{WName, W}] -> disable_worker(WName),
                        monitor_node(WName, false),
                        case dron_db:delete_worker(WName) of
                            ok    -> evict_worker(W),
                                     {reply, ok, State};
                            Error -> {reply, Error, State}
                        end; 
        []           -> {reply, {error, unknown_worker}, State}
    end;
handle_call({release_slot, WName}, _From, State) ->
    case ets:lookup(worker_records, WName) of
        [{WName, W = #worker{used_slots = USlots}}] ->
            [{_, SlotWs}] = ets:lookup(slot_workers, USlots),
            ets:insert(slot_workers, {USlots, lists:delete(WName, SlotWs)}),
            NewSlotWs = case ets:lookup(slot_workers, USlots - 1) of
                            [{_, NSlotWs}] -> [WName | NSlotWs];
                            []             -> [WName]
                        end,
            NewW = W#worker{used_slots = USlots - 1},
            dron_db:store_worker(NewW),
            ets:insert(worker_records, {WName, NewW}),
            ets:insert(slot_workers, {USlots - 1, NewSlotWs}),
            {reply, ok, State};
        [] -> {reply, {error, unknown_worker}, State}
    end;
handle_call(get_worker, _From, State) ->
    case ets:first(slot_workers) of
        '$end_of_table' -> {reply, {error, no_workers}, State};
        Slots -> MaxSlots = dron_config:max_slots(),
                 if Slots >= MaxSlots ->
                         {reply, {error, no_workers}, State};
                    true ->
                         {reply, get_worker(Slots), State}
                 end
    end;
handle_call(get_all_workers, _From, State) ->
    {reply, ets:tab2list(worker_records), State};
handle_call(Request, _From, State) ->
    error_logger:error_msg("Got unexpected call ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({master_coordinator, Master}, State) ->
    {noreply, State#state{master_coordinator = Master}};
handle_cast(Request, State) ->
    error_logger:error_msg("Got unexpected cast ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info(heartbeat, State = #state{master_coordinator = Master,
                                      worker_policy = WorkerPolicy}) ->
    rpc:cast(Master, dron_coordinator, scheduler_heartbeat,
             [node(), calendar:local_time(),
             calculate_needs(WorkerPolicy)]),
    erlang:send_after(dron_config:scheduler_heartbeat_interval(),
                      self(), heartbeat),
    {noreply, State};
handle_info({nodedown, WName}, State = #state{master_coordinator = Master}) ->
    error_logger:error_msg("Node ~p failed!~n", [WName]),
    case disable_worker(WName) of
        none   -> ok;
        Worker -> evict_worker(Worker)
    end,
    rpc:cast(Master, dron_coordinator, remove_workers, [[WName]]),
    {noreply, State};
handle_info(Request, _State) ->
    {unexpected_request, Request}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

get_worker(Slots) ->
    [{_, [WName|Aws]}] = ets:lookup(slot_workers, Slots),
    NSlots = Slots + 1,
    case ets:lookup(slot_workers, NSlots) of
        [{_, Ws}] -> ets:insert(slot_workers, {NSlots, [WName|Ws]});
        []        -> ets:insert(slot_workers, {NSlots, [WName]})
    end,
    [{_, Worker}] = ets:lookup(worker_records, WName),
    NewWorker = Worker#worker{used_slots = NSlots},
    ets:insert(worker_records, {WName, NewWorker}),
    ok = dron_db:store_worker(NewWorker),
    case Aws of
        [] -> ets:delete(slot_workers, Slots);
        _  -> ets:insert(slot_workers, {Slots, Aws})
    end,
    NewWorker.

% Deletes worker from the tree of (#slots, [workers]).
% It returns the same tree if the pair could not be found.
evict_worker(#worker{name = WName, used_slots = USlots}) ->
    [{_, Ws}] = ets:lookup(slot_workers, USlots),
    case lists:delete(WName, Ws) of
        []   -> ets:delete(slot_workers, USlots);
        Wls  -> ets:insert(slot_workers, {USlots, Wls})
    end.
   
disable_worker(WName) ->
    Ret = case ets:lookup(worker_records, WName) of
              [{WName, Worker}] -> ets:delete(worker_records, WName),
                                   ok = dron_db:store_worker(
                                          Worker#worker{enabled = false}),
                                   Worker;
              []                -> error_logger:error_msg(
                                     "Worker ~p not in-memory", [WName]),
                                   none
          end,
    % If these db writes fail then the whole worker is restarted.
    {ok, FailedJIs} = dron_db:get_job_instances_on_worker(WName),
    % TODO(ionel): Check if the proper coordinator/scheduler is called.
    lists:map(fun(JI) -> dron_coordinator:worker_disabled(JI) end, FailedJIs),
    Ret.

reconstruct_state() ->
    Ret = dron_db:get_workers_of_scheduler(node()),
    {ok, Workers} = Ret,
    lists:map(fun(Worker = #worker{name = WName, used_slots = Slots}) ->
                      monitor_node(WName, true),
                      ets:insert(worker_records, {WName, Worker}),
                      Ws = case ets:lookup(slot_workers, Slots) of
                               [{Slots, Wls}] -> [WName|Wls];
                               []             -> [WName]
                      end,                                   
                      ets:insert(slot_workers, {Slots, Ws})
              end, Workers).

calculate_needs(WorkerPolicy) ->
    {AllSlots, UsedSlots} =
        lists:unzip(lists:map(fun({_WName, #worker{max_slots = MaxSlots,
                                                   used_slots = UsedSlots}}) ->
                                      {MaxSlots, UsedSlots}
                              end, ets:tab2list(worker_records))),
    NSlots = lists:sum(AllSlots),
    NUsedSlots = lists:sum(UsedSlots),
    Load = compute_load(NSlots, NUsedSlots),
    Policy = apply(dron_config, WorkerPolicy, []),
    {OffersBound, RequestsBound} = Policy,
    if
        Load < OffersBound ->
            compute_offers(NSlots, NUsedSlots, Policy, 0);
        Load > RequestsBound ->
            compute_requests(NSlots, NUsedSlots, Policy, 0);
        true ->
            alive
    end.

compute_load(NSlots, NUsedSlots) ->
    if
        NSlots > 0 -> NUsedSlots / NSlots;
        true       -> 1
    end.

compute_offers(NSlots, NUsedSlots, {OffersBound, RequestsBound} = Policy,
               NumOffers) ->
    Load = compute_load(NSlots, NUsedSlots),
    if
        Load < OffersBound ->
            compute_offers(NSlots - dron_config:max_slots(),
                           NUsedSlots, Policy, NumOffers + 1);
        true ->
            NOffers = if
                          Load > RequestsBound -> NumOffers - 1;
                          true -> NumOffers
                      end,
            case NOffers of
                0 -> alive;
                _ -> {offer, NOffers}
            end
    end.

compute_requests(NSlots, NUsedSlots, {_OffersBound, RequestsBound} = Policy,
                 NumRequests) ->
    Load = compute_load(NSlots, NUsedSlots),
    if
        Load > RequestsBound ->
            compute_requests(NSlots + dron_config:max_slots(),
                             NUsedSlots, Policy, NumRequests + 1);
        true ->
            case NumRequests of
                0 -> alive;
                _ -> {request, NumRequests}
            end
    end.
