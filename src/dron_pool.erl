-module(dron_pool).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, add_worker/1, add_worker/2, auto_add_workers/0,
         remove_worker/1, get_worker/0, release_worker_slot/1]).

%===============================================================================

start_link() ->
    gen_server:start_link({global, ?NAME}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% Add a new worker to the pool. The worker will have the number of slots
%% defined in dron_config.
%%
%% @spec add_worker(WorkerName) -> ok | {error, no_connection} | Error
%% @end
%%------------------------------------------------------------------------------
add_worker(WName) ->
    gen_server:call({global, ?NAME}, {add, WName, dron_config:max_slots()}).

%%------------------------------------------------------------------------------
%% @doc
%% Add a new worker to the pool.
%%
%% @spec add_worker(WorkerName, MaxSlots) -> ok | {error, no_connection} | Error
%% @end
%%------------------------------------------------------------------------------
add_worker(WName, MaxSlots) ->
    gen_server:call({global, ?NAME}, {add, WName, MaxSlots}).

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
%% Removes a worker from the pool.
%%
%% @spec remove_worker(WorkerName) -> ok | {error, unknown_worker} | Error
%% @end
%%------------------------------------------------------------------------------
remove_worker(WName) ->
    gen_server:call({global, ?NAME}, {remove, WName}).

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
    gen_server:call({global, ?NAME}, get_worker).

%%------------------------------------------------------------------------------
%% @doc
%% Releases a slot on a given worker.
%%
%% @spec release_worker_slot(WorkerName) -> ok | {error, unknown_worker}
%% @end
%%------------------------------------------------------------------------------
release_worker_slot(WName) ->
    gen_server:call({global, ?NAME}, {release_slot, WName}).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    ets:new(worker_records, [named_table]),
    ets:new(slot_workers, [ordered_set, named_table]),
    reconstruct_state(),
    {ok, []}.

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
                        NewW = #worker{name = WName, enabled = true,
                                       max_slots = Slots, used_slots = 0},
                        case dron_db:store_worker(NewW) of
                            ok    -> ets:insert(worker_records, {WName, NewW}),
                                     {reply, ok, State};
                            Error -> {reply, Error, State}
                        end;
                _    -> {reply, {error, no_connection}, State}
            end
    end;
handle_call({remove, WName}, _From, State) ->
    case ets:lookup(worker_records, WName) of
        [{WName, W}] -> disable_worker(WName),
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
        Slots -> [{_, [WName|Aws]}] = ets:lookup(slot_workers, Slots),
                 NSlots = Slots + 1,
                 case ets:lookup(slot_workers, NSlots) of
                     [{_, Ws}] -> ets:insert(slot_workers,
                                             {NSlots, [WName|Ws]});
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
                 {reply, NewWorker, State}
    end;
handle_call(Request, _From, _State) ->
    {unexpected_request, Request}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(_Request, _State) ->
    not_implemented.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({nodedown, WName}, State) ->
    error_logger:error_msg("Node ~p failed!~n", [WName]),
    case disable_worker(WName) of
        none   -> ok;
        Worker -> evict_worker(Worker)
    end,
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

% TODO(ionel): Implement worker monitoring.
reconstruct_state() ->
    {ok, Workers} = dron_db:get_workers(true),
    lists:map(fun(Worker = #worker{name = WName, used_slots = Slots}) ->
                      ets:insert(worker_records, {WName, Worker}),
                      Ws = case ets:lookup(slot_workers, Slots) of
                               [{Slots, Wls}] -> [WName|Wls];
                               []             -> [WName]
                      end,                                   
                      ets:insert(slot_workers, {Slots, Ws})
              end, Workers).
