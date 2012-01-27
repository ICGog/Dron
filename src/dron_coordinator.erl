-module(dron_coordinator).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_leader).

-export([init/1, handle_call/4, handle_cast/3, handle_info/2,
         handle_leader_call/4, handle_leader_cast/3, handle_DOWN/3,
         elected/3, surrendered/3, from_leader/3, code_change/4, terminate/2]).

-export([start_link/1, schedule/1, unschedule/1]).

-export([job_instance_succeeded/1, job_instance_failed/2,
         job_instance_timeout/1, job_instance_killed/1,
         dependency_satisfied/1, worker_disabled/1]).

-export([scheduler_heartbeat/3, new_scheduler_leader/2, start_new_workers/2,
        start_new_scheduler/2, add_workers/2, add_scheduler/2,
        remove_scheduler/1, remove_workers/1, remove_workers/2,
        auto_add_sched_workers/0, get_workers/1, release_slot/1]).

-record(state, {leader, schedulers}).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts the coordinator on a list of nodes. One node will be elected as
%% leader.
%%
%% @spec start_link(MasterNodes) -> ok
%% @end
%%------------------------------------------------------------------------------
start_link(MasterNodes) ->
    gen_leader:start_link(?MODULE, MasterNodes, [], ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% @spec schedule(Job) -> ok
%% @end
%%------------------------------------------------------------------------------
schedule(Job) ->
    gen_leader:leader_cast(?MODULE, {schedule, Job}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec unschedule(JobName) -> ok
%% @end
%%------------------------------------------------------------------------------
unschedule(JName) ->
    gen_leader:leader_cast(?MODULE, {unschedule, JName}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec job_instance_succeeded(JobInstanceId) -> ok
%% @end
%%------------------------------------------------------------------------------
job_instance_succeeded(JId) ->
    gen_leader:leader_cast(?MODULE, {succeeded, JId}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec job_instance_failed(JobInstanceId, Reason) -> ok
%% @end
%%------------------------------------------------------------------------------
job_instance_failed(JId, Reason) ->
    gen_leader:leader_cast(?MODULE, {failed, JId, Reason}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec job_instance_timeout(JobInstanceId) -> ok
%% @end
%%------------------------------------------------------------------------------
job_instance_timeout(JId) ->
    gen_leader:leader_cast(?MODULE, {timeout, JId}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec job_instance_killed(JobInstanceId) -> ok
%% @end
%%------------------------------------------------------------------------------
job_instance_killed(JId) ->
    gen_leader:leader_cast(?MODULE, {killed, JId}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec dependency_satisfied(ResourceId) -> ok
%% @end
%%------------------------------------------------------------------------------
dependency_satisfied(RId) ->
    gen_leader:leader_cast(?MODULE, {satisfied, RId}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec worker_disabled(JobInstance) -> ok
%% @end
%%------------------------------------------------------------------------------
worker_disabled(JI) ->
    gen_leader:leader_cast(?MODULE, {worker_disabled, JI}).

%%------------------------------------------------------------------------------
%% @doc
%% Schedulers use this method to inform the coordinator about their requirement.
%%
%% @spec scheduler_heartbeat(SchedulerName, Time, Requirement) -> ok
%% @end
%%------------------------------------------------------------------------------
scheduler_heartbeat(SchedulerName, Time, Requirement) ->
    gen_leader:leader_cast(?MODULE, {scheduler_heartbeat, SchedulerName,
                                     Time, Requirement}).

%%------------------------------------------------------------------------------
%% @doc
%% Schedulers use this method to inform the coordinator about a new leader.
%%
%% @spec new_scheduler_leader(OldScheduler, NewScheduler) -> ok
%% @end
%%------------------------------------------------------------------------------
new_scheduler_leader(OldSched, NewSched) ->
    gen_leader:leader_cast(?MODULE, {new_scheduler_leader, OldSched, NewSched}).

%%------------------------------------------------------------------------------
%% @doc
%% Starts a list of new workers. Returns a list containing the workers that have
%% been successfully started.
%%
%% @spec start_new_workers(SchedulerName, Workers) ->
%%     NewWorkers | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
start_new_workers(SName, Workers) ->
    gen_leader:leader_call(?MODULE, {start_new_workers, SName, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Starts a new scheduler together with a list of new workers.
%%
%% @spec start_new_scheduler(SchedulerName, Workers) ->
%%     NewWorkers | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
start_new_scheduler(SName, Workers) ->
    gen_leader:leader_call(?MODULE, {start_new_scheduler, SName, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Adds a list of already existing workers to a scheduler. The ones managed
%% by other schedulers are ignored.
%%
%% @spec add_workers(SchedulerName, Workers) ->
%%     AddedWorkers | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
add_workers(SName, Workers) ->
    gen_leader:leader_call(?MODULE, {add_workers, SName, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Adds a scheduler and a list of already existing workers. The workers managed
%% by other schedulers are ignored.
%%
%% @spec add_scheduler(SchedulerName, Workers) ->
%%     AddedWorkers | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
add_scheduler(SName, Workers) ->
    gen_leader:leader_call(?MODULE, {add_scheduler, SName, Workers}).    

%%------------------------------------------------------------------------------
%% @doc
%% Remove a scheduler from the coordinator.
%%
%% @spec remove_scheduler(SchedulerName) ->
%%     RemovedWorkers | {error, unknown_scheduler}
%% @end
%%------------------------------------------------------------------------------
remove_scheduler(SName) ->
    gen_leader:leader_call(?MODULE, {remove_scheduler, SName}).

%%------------------------------------------------------------------------------
%% @doc
%% Removes workers. It ignores the one that do not exist.
%%
%% @spec remove_workers(Workers) -> RemovedWorkers
%% @end
%%------------------------------------------------------------------------------
remove_workers(Workers) ->
    gen_leader:leader_call(?MODULE, {remove_workers, Workers}).

%%------------------------------------------------------------------------------
%% @doc
%% Removes a specified number of workers from a given scheduler.
%%
%% @spec remove_workers(SchedulerName, Number) -> RemovedWorkers
%% @end
%%------------------------------------------------------------------------------
remove_workers(SName, Number) ->
    gen_leader:leader_call(?MODULE, {remove_workers, SName, Number}).

%%------------------------------------------------------------------------------
%% @doc
%% Adds the schedulers and workers defined in the environemnt variables.
%%
%% @spec auto_add_sched_workers() -> ok
%% @end
%%------------------------------------------------------------------------------
auto_add_sched_workers() ->
    gen_leader:leader_call(?MODULE, auto_add_sched_workers).

%%------------------------------------------------------------------------------
%% @doc
%% Returns the workers assigned to a scheduler.
%%
%% @spec get_workers(SName) -> {ok, Workers} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_workers(SName) ->
    gen_leader:leader_call(?MODULE, {get_workers, SName}).

%%------------------------------------------------------------------------------
%% @doc
%% Release a slot on a given worker. Called by the scheduler when it is not
%% managing the worker.
%%
%% @spec release_slot(WName) -> ok
%% @end
%%------------------------------------------------------------------------------
release_slot(WName) ->
    gen_leader:leader_cast(?MODULE, {release_slot, WName}).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    ets:new(scheduler_heartbeat, [named_table]),
    ets:new(scheduler_assig, [named_table]),
    ets:new(worker_scheduler, [named_table]),
    {ok, #state{schedulers = []}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
elected(State = #state{schedulers = Schedulers}, _Election, undefined) ->
    error_logger:info_msg("~p elected as master", [node()]),
    %% Inform the schedulers who is the new master.
    lists:map(fun(Scheduler) ->
                      rpc:cast(Scheduler, dron_scheduler, master_coordinator,
                               [node()])
              end, Schedulers),
    erlang:send_after(dron_config:scheduler_heartbeat_interval(), self(),
                     balance_workers),
    {ok, [], State#state{leader = true}};
elected(State, _Election, _Node) ->
    {reply, [], State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
surrendered(State, _Sync, _Election) ->
    {ok, State#state{leader = false}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_leader_call({start_new_workers, SName, Workers}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case dron_monitor:start_new_workers(Schedulers, SName, Workers) of
        {error, Reason} -> {reply, {error, Reason}, State};
        NewWs           -> {reply, NewWs, {start_new_workers, SName, NewWs},
                            State}
    end;
handle_leader_call({start_new_scheduler, SName, Workers}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case dron_monitor:start_new_scheduler(Schedulers, SName, Workers) of
        {error, Reason} -> {reply, {error, Reason}, State};
        NewWs           -> {reply, NewWs, {start_new_scheduler, SName, NewWs},
                            State#state{schedulers = [SName | Schedulers]}}
    end;
handle_leader_call({add_workers, SName, Workers}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case lists:member(SName, Schedulers) of
        false -> {error, unknown_scheduler};
        true  -> AddedWs = dron_monitor:add_workers(SName, Workers),
                 {reply, AddedWs, {add_workers, SName, AddedWs}, State}
    end;
handle_leader_call({add_scheduler, SName, Workers}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case dron_monitor:add_scheduler(Schedulers, SName, Workers) of
        {error, Reason} -> {reply, {error, Reason}, State};
        AddedWs         -> {reply, AddedWs, {add_scheduler, SName, AddedWs},
                            State#state{schedulers = [SName | Schedulers]}}
    end;
handle_leader_call({remove_scheduler, SName}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case dron_monitor:remove_scheduler(Schedulers, SName) of
        {error, Reason} -> {reply, {error, Reason}, State};
        RemWs           -> {reply, RemWs, {remove_scheduler, SName, RemWs},
                            State#state{schedulers = lists:delete(SName,
                                                                  Schedulers)}}
    end;
handle_leader_call({remove_workers, Workers}, _From, State, _Election) ->
    RemWs = dron_monitor:remove_workers(Workers),
    {reply, RemWs, {remove_workers, RemWs}, State};
handle_leader_call({remove_workers, SName, WNum}, _From, State, _Election) ->
    RemWs = dron_monitor:remove_num_workers(SName, WNum),
    {reply, RemWs, {remove_workers, SName, RemWs}, State};
handle_leader_call(auto_add_sched_workers, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    SchedWorkers = dron_monitor:assign_workers(dron_config:scheduler_nodes()),
    error_logger:info_msg("~nDron schedulers assignments: ~p~n",
                          [SchedWorkers]),
    error_logger:info_msg("~nDron schedulers are starting...~n", []),
    OkSched = lists:filter(
        fun({SName, Workers}) ->
                case dron_monitor:start_new_scheduler(Schedulers,
                                                      SName, Workers) of
                    {error, Reason} ->
                        error_logger:error_msg("Failed to start ~p because ~p",
                                               [SName, Reason]),
                        false;
                    Ws              ->
                        error_logger:info_msg(
                          "Started workers ~p for scheduler ~p", [Ws, SName]),
                        true
                end
        end, SchedWorkers),
    error_logger:info_msg("~nStarted schedulers: ~p~n", [OkSched]),
    {NewSched, _} = lists:unzip(OkSched),
    {reply, ok, {auto_add_sched_workers, NewSched, OkSched},
     State#state{schedulers = lists:append(Schedulers, NewSched)}};
handle_leader_call({get_workers, SName}, _From,
                   State = #state{schedulers = Schedulers}, _Election) ->
    case lists:member(SName, Schedulers) of
        false -> {reply, {error, unknown_scheduler}, State};
        true  -> Reply = case ets:lookup(scheduler_assig, SName) of
                             [] -> {error, no_sched_entry};
                             [{_, Ws}] -> {ok, Ws}
                         end,
                 {reply, Reply, State}
    end;
handle_leader_call(Request, _From, State, _Election) ->
    error_logger:error_msg("Unexpected leader call ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_leader_cast({schedule, Job = #job{name = Name}},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(Name, Schedulers), dron_scheduler, schedule, [Job]),
    {ok, State};
handle_leader_cast({unschedule, JName},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(JName, Schedulers), dron_scheduler, unschedule,
             [JName]),
    {ok, State};
handle_leader_cast({succeeded, JId = {Name, _Date}},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(Name, Schedulers), dron_scheduler,
             job_instance_succeeded, [JId]),
    {ok, State};
handle_leader_cast({failed, JId = {Name, _Date}, Reason},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(Name, Schedulers), dron_scheduler,
             job_instance_failed, [JId, Reason]),
    {ok, State};
handle_leader_cast({timeout, JId = {Name, _Date}},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(Name, Schedulers), dron_scheduler,
             job_instance_timeout, [JId]),
    {ok, State};
handle_leader_cast({killed, JId = {Name, _Date}},
                   State = #state{schedulers = Schedulers}, _Election) ->
    rpc:call(dron_hash:hash(Name, Schedulers), dron_scheduler,
             job_instance_killed, [JId]),
    {ok, State};
handle_leader_cast({satisfied, _RId}, State, _Election) ->
    %% TODO(ionel): Do the call to the appropriate scheduler.
    {ok, State};
handle_leader_cast({worker_disabled, _JI}, State, _Election) ->
    %% TODO(ionel): Do the call to the appropriate scheduler.
    {ok, State};
%% Updates the load of a scheduler. It is called by the scheduler.
handle_leader_cast({scheduler_heartbeat, SName, Time, Req}, State, _Election) ->
    error_logger:info_msg("Got scheduler ~p heartbeat ~p", [SName, Req]),
    ets:insert(scheduler_heartbeat, {SName, {Req, Time}}),
    {ok, {scheduler_heartbeat, SName, Time, Req}, State};
handle_leader_cast({new_scheduler_leader, OldSched, NewSched},
                   State = #state{schedulers = Schedulers}, _Election) ->
    dron_monitor:new_scheduler_leader(OldSched, NewSched),
    case dron_db:update_workers_scheduler(OldSched, NewSched) of
        ok              -> ok;
        {error, Reason} ->
            error_logger:error_msg("Could not update workers" ++
                                       " from ~p to ~p because of ~p",
                                   [OldSched, NewSched, Reason])
    end,
    {ok, {new_scheduler_leader, OldSched, NewSched},
     State#state{schedulers = [NewSched | lists:delete(OldSched, Schedulers)]}};
handle_leader_cast({release_slot, WName}, State, _Election) ->
    case ets:lookup(worker_scheduler, WName) of
        [{_, SName}] ->
            rpc:cast(SName, dron_pool, release_worker_slot, [WName]);
        [{_, unallocated}] ->
            dron_db:adjust_slot(WName, -1);
        [] ->
            error_logger:info_msg("Unknown worker ~p", [WName])
    end,
    {ok, State};
handle_leader_cast(_Request, State, _Election) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
from_leader({scheduler_heartbeat, SName, Time, Req}, State, _Election) ->
    ets:insert(scheduler_heartbeat, {SName, {Req, Time}}),
    {ok, State};
from_leader({new_scheduler_leader, OldSched, NewSched},
            State = #state{schedulers = Schedulers}, _Election) ->
    dron_monitor:new_scheduler_leader(OldSched, NewSched),
    {ok, State#state{
           schedulers = [NewSched | lists:delete(OldSched, Schedulers)]}};
from_leader({start_new_workers, SName, Workers}, State, _Election) ->
    dron_monitor:store_new_workers(SName, Workers),
    {ok, State};
from_leader({start_new_scheduler, SName, Workers},
            State = #state{schedulers = Schedulers}, _Election) ->
    ets:insert(scheduler_heartbeat, {SName, {alive, calendar:local_time()}}),
    dron_monitor:store_new_workers(SName, Workers),
    {ok, State#state{schedulers = [SName | Schedulers]}};
from_leader({add_workers, SName, Workers}, State, _Election) ->
    dron_monitor:store_new_workers(SName, Workers),
    {ok, State};
from_leader({add_scheduler, SName, AddedWs},
            State = #state{schedulers = Schedulers}, _Election) ->
    ets:insert(scheduler_heartbeat, {SName, {alive, calendar:local_time()}}),
    dron_monitor:store_new_workers(SName, AddedWs),
    {ok, State#state{schedulers = [SName | Schedulers]}};    
from_leader({remove_scheduler, SName, RemWs},
            State = #state{schedulers = Schedulers}, _Election) ->
    ets:delete(scheduler_heartbeat, SName),
    ets:delete(scheduler_assig, SName),
    lists:map(fun(W) ->
                      ets:insert(worker_scheduler, {W, unallocated})
              end, RemWs),
    {ok, State#state{schedulers = lists:delete(SName, Schedulers)}};
from_leader({remove_workers, RemWs}, State, _Election) ->
    dron_monitor:remove_workers(RemWs),
    {ok, State};
from_leader({remove_workers, SName, RemWs}, State, _Election) ->
    dron_monitor:remove_workers(SName, RemWs),
    {ok, State};
from_leader({auto_add_sched_workers, NewSched, OkSched},
            State = #state{schedulers = Schedulers}, _Election) ->
    lists:map(fun({SName, Workers}) ->
                      dron_monitor:add_scheduler(Schedulers, SName, Workers)
              end, OkSched),
    {ok, State#state{schedulers = lists:append(Schedulers, NewSched)}};
from_leader(_Request, State, _Election) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_DOWN(Node, State, _Election) ->
    error_logger:error_msg("Master node ~p went down", [Node]),
    {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(Request, _From, State, _Election) ->
    error_logger:error_msg("Got unexpected call ~p", [Request]),
    {stop, not_supported, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(Msg, State, _Election) ->
    error_logger:error_msg("Got unexpected cast ~p", [Msg]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info(balance_workers, State = #state{leader = false}) ->
    % Ignore the message.
    {noreply, State};
handle_info(balance_workers, State = #state{leader = true}) ->
    error_logger:info_msg("Balancing workers: ~p",
                          [ets:tab2list(scheduler_assig)]),
    erlang:spawn_link(dron_monitor, balance_workers,
                      [ets:tab2list(scheduler_heartbeat)]),
    erlang:send_after(dron_config:scheduler_heartbeat_interval(), self(),
                      balance_workers),
    {noreply, State};
handle_info(Info, State) ->
    error_logger:error_msg("Got unexpected message ~p", [Info]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Election, _Extra) ->
    {ok, State}.    
