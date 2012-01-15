-module(dron_scheduler).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_leader).

-export([init/1, handle_call/4, handle_cast/3, handle_info/2,
         handle_leader_call/4, handle_leader_cast/3, handle_DOWN/3,
         elected/3, surrendered/3, from_leader/3, code_change/4, terminate/2,
         create_job_instance/3, create_job_instance/4, run_instance/2,
         ji_succeeded/1, ji_killed/1, ji_timeout/1, ji_failed/2,
         run_job_instance/2]).

-export([start_link/1, schedule/1, unschedule/1]).

-export([job_instance_succeeded/1, job_instance_failed/2,
         job_instance_timeout/1, job_instance_killed/1,
         dependency_satisfied/1, worker_disabled/1,
         store_waiting_job_instance_timer/2,
         store_waiting_job_instance_deps/2]).

-record(state, {leader}).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts the the scheduler on a list of nodes. One node will be elected as
%% leader.
%%
%% @spec start_link(Nodes) -> ok
%% @end
%%------------------------------------------------------------------------------
start_link(Nodes) ->
    gen_leader:start_link(?MODULE, Nodes, [], ?MODULE, [], []).

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
%% @spec store_waiting_job_instance_timer(JobInstanceId, WaitTimerRef) -> ok
%% @end
%%------------------------------------------------------------------------------
store_waiting_job_instance_timer(JId, TRef) ->
    gen_leader:cast(?MODULE, {waiting_job_instance_timer, JId, TRef}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_waiting_job_instance_deps(JobInstanceId,
%%        UnsatisfiedDependencies) -> ok
%% @end
%%------------------------------------------------------------------------------
store_waiting_job_instance_deps(JId, UnsatisfiedDeps) ->
    gen_leader:leader_cast(?MODULE, {waiting_job_instance_deps, JId,
                                     UnsatisfiedDeps}).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    ets:new(schedule_timers, [named_table]),
    ets:new(start_timers, [named_table]),
    ets:new(wait_timers, [named_table]),
    ets:new(ji_deps, [named_table]),
    ets:new(delay, [public, named_table]),
    {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Called only in the leader process when it is elected. Sync will be
%% broadcasted to all the nodes in the cluster.
%%
%% @spec elected(State, Election, undefined) -> {ok, Synch, State}
%% @end
%%------------------------------------------------------------------------------
elected(State, _Election, undefined) ->
    error_logger:info_msg("~p elected as master", [node()]),
    dron_pool:start_link(),
    {ok, [], State#state{leader = true}};
%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Called only in the leader process when a new candidate joins the cluster.
%% Sync will be sent to the Node.
%%
%% @spec elected(State, Election, Node) -> {ok, Synch, State}
%% @end
%%------------------------------------------------------------------------------
elected(State, _Election, _Node) ->
    {reply, [], State}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Called in all members of the cluster except the leader. Synch is a term
%% returned by the leader in the elected/3 callback.
%%
%% @spec surrendered(State, Synch, Election) -> {ok, State}
%% @end
%%------------------------------------------------------------------------------
surrendered(State, _Sync, _Election) ->
    {ok, State#state{leader = false}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_leader_call(Request, _From, State, _Election) ->
    error_logger:error_msg("Unexpected leader call ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_leader_cast({schedule, Job = #job{name = JName, start_time = STime}},
                   State, _Election) ->
    % TODO(ionel): If the process fails while running old instances then
    % some of them may be re-run. Fix it.
    AfterT = run_job_instances_up_to_now(
               Job,
               self(),
               calendar:datetime_to_gregorian_seconds(calendar:local_time()),
               calendar:datetime_to_gregorian_seconds(STime)),
    ets:insert(start_timers, {JName, erlang:send_after(AfterT * 1000, self(),
                                                       {schedule, Job})}),
    {ok, {schedule, Job, AfterT}, State};
handle_leader_cast({unschedule, JName}, State, _Election) ->
    unschedule_job_inmemory(JName),
    ok = dron_db:archive_job(JName),
    {ok, {unschedule, JName}, State};
% A failing leader can potentially take down many processes that are running
% ji_succeeded,ji_failed... Fix it.
handle_leader_cast({succeeded, JId}, State, _Election) ->
    erlang:spawn_link(?MODULE, ji_succeeded, [JId]),
    {noreply, State};
handle_leader_cast({failed, JId, Reason}, State, _Election) ->
    error_logger:error_msg("~p failed with ~p", [JId, Reason]),
    erlang:spawn_link(?MODULE, ji_failed, [JId, true]),
    {noreply, State};
handle_leader_cast({timeout, JId}, State, _Election) ->
    erlang:spawn_link(?MODULE, ji_timeout, [JId]),
    {noreply, State};
handle_leader_cast({killed, JId}, State, _Election) ->
    erlang:spawn_link(?MODULE, ji_killed, [JId]),
    {noreply, State};
handle_leader_cast({satisfied, RId}, State, _Election) ->
    ok = dron_db:set_resource_state(RId, satisfied),
    {ok, Dependants} = dron_db:get_dependants(RId),
    lists:map(fun(#resource_deps{dep = JId}) ->
                      satisfied_dependency(RId, JId, true) end, Dependants),
    {ok, {satisfied, RId}, State};
handle_leader_cast({worker_disabled, JI}, State, _Election) ->
    erlang:spawn_link(?MODULE, run_job_instance, [JI, true]),
    {ok, {worker_disabled, JI}, State};
handle_leader_cast({waiting_job_instance_deps, JId, UnsatisfiedDeps}, State,
                   _Election) ->
    ets:insert(ji_deps, {JId, UnsatisfiedDeps}),
    {ok, {waiting_job_instance_deps, UnsatisfiedDeps}, State};
handle_leader_cast(Request, State, _Election) ->
    error_logger:error_msg("Unexpected leader cast ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Handling messages from leader.
%%
%% @spec from_leader(Request, State, Election) ->
%%                                     {ok, State} |
%%                                     {noreply, State} |
%%                                     {stop, Reason, State}
%% @end
%%------------------------------------------------------------------------------
from_leader({schedule, Job = #job{name = JName}, AfterT}, State, _Election) ->
    ets:insert(start_timers, {JName, erlang:send_after(AfterT * 1000, self(),
                                                       {schedule, Job})}),
    {ok, State};
from_leader({unschedule, JName}, State, _Election) ->
    unschedule_job_inmemory(JName),
    {ok, State};
from_leader({satisfied, RId}, State, _Election) ->
    {ok, Dependants} = dron_db:get_dependants(RId),
    lists:map(fun(JId) -> satisfied_dependency(RId, JId, false) end,
              Dependants),
    {ok, State};
from_leader({worker_disabled, JI}, State, _Election) ->
    erlang:spawn_link(?MODULE, run_job_instance, [JI, false]),
    {ok, State};
from_leader({waiting_job_instance_deps, JId, UnsatisfiedDeps}, State,
            _Election) ->
    ets:insert(ji_deps, {JId, UnsatisfiedDeps}),
    {ok, State};
from_leader(_Request, State, _Election) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Handling nodes going down. Called in the leader only.
%%
%% @spec handle_DOWN(Node, State, Election) ->
%%                                  {ok, State} |
%%                                  {ok, Broadcast, State}
%% @end
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
handle_cast({waiting_job_instance_timer, JId, TRef}, State, _Election) ->
    ets:insert(wait_timers, {JId, TRef}),
    {noreply, State};
handle_cast(Msg, State, _Election) ->
    error_logger:errog_msg("Got unexpected cast ~p", [Msg]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({schedule, Job = #job{name = JName, frequency = Freq}},
            State = #state{leader = Leader}) ->
    {ok, TRef} = timer:apply_interval(Freq * 1000, ?MODULE, create_job_instance,
                                      [Job, self(), Leader]),
    ets:insert(schedule_timers, {JName, TRef}),
    ets:delete(start_timers, JName),
    {noreply, State};
handle_info({wait_timeout, JId}, State) ->
    case ets:lookup(wait_timers, JId) of
        [{JId, TRef}] -> erlang:cancel_timer(TRef),
                         ets:delete(wait_timers, JId);
        []            -> ok
    end,
    {noreply, State};
handle_info({'EXIT', _Pid, normal}, State) ->
    % Linked process finished normally. Ignore the message.
    {noreply, State};
handle_info({'EXIT', PId, Reason}, State) ->
    % TODO(ionel): Handle child process failure.
    error_logger:error_msg("~p anormally finished with ~p", [PId, Reason]),
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

instanciate_dependencies(_JId, []) ->
    {[], []};
instanciate_dependencies(JId, Dependencies) ->
    % TODO(ionel): Handle dependencies instanciation.
    {Dependencies, dron_db:store_dependant(Dependencies, JId)}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
create_job_instance(Job, SchedulerPid, Leader) ->
    create_job_instance(Job, calendar:local_time(), SchedulerPid, Leader).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
create_job_instance(#job{name = Name, deps_timeout = DepsTimeout,
                         dependencies = Dependencies},
                    Date, SchedulerPid, false) ->
    JId = {Name, Date},
    case Dependencies of
        [] -> ok;
        _  -> TRef = erlang:send_after(DepsTimeout * 1000, SchedulerPid,
                                       {wait_timeout, JId}),
              dron_scheduler:store_waiting_job_instance_timer(JId, TRef)
    end;
create_job_instance(#job{name = Name, cmd_line = Cmd, timeout = Timeout,
                         deps_timeout = DepsTimeout,
                         dependencies = Dependencies},
                    Date, SchedulerPid, true) ->
    JId = {Name, Date},
    RunTime = calendar:local_time(),
    Delay = calendar:datetime_to_gregorian_seconds(RunTime) -
        calendar:datetime_to_gregorian_seconds(Date),
    MaxDelay = case ets:lookup(delay, delay) of
                   [{delay, MDelay}] -> MDelay;
                   []                -> 0
               end,
    if Delay > MaxDelay -> ets:insert(delay, {delay, Delay}),
                           error_logger:info_msg("Max Delay ~p", [Delay]);
       true             -> ok
    end,
    {Deps, UnsatisfiedDeps} = instanciate_dependencies(JId, Dependencies),
    JI =  #job_instance{jid = JId, name = Name, cmd_line = Cmd,
                        state = waiting, timeout = Timeout,
                        run_time = RunTime,
                        num_retry = 0,
                        dependencies = Deps,
                        worker = undefined},
    ok = dron_db:store_job_instance(JI),
    case UnsatisfiedDeps of 
        [] -> run_job_instance(JI, true);
        _  -> TRef = erlang:send_after(DepsTimeout * 1000, SchedulerPid,
                                       {wait_timeout, JId}),
              dron_scheduler:store_waiting_job_instance_timer(JId, TRef),
              dron_scheduler:store_waiting_job_instance_deps(
                JId, UnsatisfiedDeps)
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
run_instance(_JId, false) ->
    ok;
run_instance(JId, true) ->
    {ok, NoWorkerJI = #job_instance{timeout = Timeout}} =
        dron_db:get_job_instance_unsync(JId),
    #worker{name = WName} = dron_pool:get_worker(),
    JI = NoWorkerJI#job_instance{worker = WName},
    ok = dron_db:store_job_instance(JI),
    dron_worker:run(WName, JI, Timeout).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
run_job_instance(_JI, false) ->
    ok;
run_job_instance(JobInstance = #job_instance{timeout = Timeout}, true) ->
    #worker{name = WName} = dron_pool:get_worker(),
    WorkerJI = JobInstance#job_instance{worker = WName},
    ok = dron_db:store_job_instance(WorkerJI),
    dron_worker:run(WName, WorkerJI, Timeout).
        
run_job_instances_up_to_now(Job = #job{frequency = Frequency}, SchedulerPid,
                            Now, STime) ->
    if STime < Now ->
            create_job_instance(
              Job, calendar:gregorian_seconds_to_datetime(STime),
              SchedulerPid, true),
            run_job_instances_up_to_now(
              Job,
              SchedulerPid,
              calendar:datetime_to_gregorian_seconds(calendar:local_time()),
              STime + Frequency);
       true        -> STime - Now
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
satisfied_dependency(RId, JId, Leader) ->
    case ets:lookup(ji_deps, JId) of
        [{JId, RIds}] ->
            case lists:delete(RId, RIds) of
                []  -> case ets:lookup(wait_timers, JId) of
                           [{JId, TRef}] -> erlang:cancel_timer(TRef),
                                            ets:delete(wait_timers, JId);
                           []            -> ok 
                       end,
                       run_instance(JId, Leader),
                       ets:delete(ji_deps, JId);
                Val -> ets:insert(ji_deps, {JId, Val})
            end;
        []            ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
ji_succeeded(JId) ->
    ok = dron_db:set_job_instance_state(JId, succeeded),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance_unsync(JId),
    ok = dron_pool:release_worker_slot(WName),
    % TODO(ionel): Move this out to consumer. (Think about satisfing various
    % resources)
    dependency_satisfied(JId).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
ji_killed(JId) ->
    ok = dron_db:set_job_instance_state(JId, killed),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance_unsync(JId),
    ok = dron_pool:release_worker_slot(WName).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
ji_timeout(JId) ->
    ok = dron_db:set_job_instance_state(JId, timeout),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance_unsync(JId),
    ok = dron_pool:release_worker_slot(WName).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
ji_failed(JId, Leader) ->
    {ok, JI = #job_instance{name = JName, worker = WName, num_retry = NumRet}} =
        dron_db:get_job_instance_unsync(JId),
    ok = dron_pool:release_worker_slot(WName),
    {ok, #job{max_retries = MaxRet}} = dron_db:get_job_unsync(JName),
    if
        NumRet < MaxRet ->
            run_job_instance(JI#job_instance{num_retry = NumRet + 1}, Leader);
        true            ->
            ok
    end.

unschedule_job_inmemory(JName) ->
    case ets:lookup(start_timers, JName) of
        [{JName, STRef}] -> erlang:cancel_timer(STRef),
                            ets:delete(start_timers, JName);
        []               -> ok
    end,
    case ets:lookup(schedule_timers, JName) of
        [{JName, TRef}]  -> timer:cancel(TRef),
                            ets:delete(schedule_timers, JName);
        []               -> ok
    end.
