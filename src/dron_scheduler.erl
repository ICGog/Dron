-module(dron_scheduler).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, create_job_instance/1, create_job_instance/2,
         run_instance/1]).

-export([start_link/0, schedule/1, unschedule/1]).

%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

schedule(Job) ->
    gen_server:cast(?NAME, {schedule, Job}).

unschedule(JName) ->
    gen_server:cast(?NAME, {unschedule, JName}).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

instanciate_dependencies(RId, []) ->
    [];
instanciate_dependencies(RId, Dependencies) ->
    % TODO(ionel): Handle dependencies instanciation.
    ok = dron_db:store_dependant(Dependencies, RId),
    Dependencies.

create_job_instance(Job) ->
    create_job_instance(Job, calendar:local_time()).

create_job_instance(#job{name = Name, cmd_line = Cmd, timeout = Timeout,
                         deps_timeout = DepsTimeout,
                         dependencies = Dependencies}, Date) ->
    JId = {Name, Date},
    JI =  #job_instance{jid = JId, name = Name, cmd_line = Cmd, state = waiting,
                        timeout = Timeout, run_time = calendar:local_time(),
                        num_retry = 0,
                        dependencies = instanciate_dependencies(JId,
                                                                Dependencies),
                        worker = undefined},
    ok = dron_db:store_job_instance(JI),
    case Dependencies of 
        [] -> run_instance(JId);
        _  -> TRef = erlang:send_after(DepsTimeout * 1000, self(),
                                       {wait_timeout, JId}),
              dron_scheduler ! {waiting_job_instance, JId, TRef, Dependencies}
    end.

run_instance(JId) ->
    {ok, NoWorkerJI = #job_instance{timeout = Timeout}} =
        dron_db:get_job_instance(JId),
    #worker{name = WName} = dron_pool:get_worker(),
    JI = NoWorkerJI#job_instance{worker = WName},
    ok = dron_db:store_job_instance(JI),
    dron_worker:run(WName, JI, Timeout).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    ets:new(schedule_timers, [named_table]),
    ets:new(start_timers, [named_table]),
    ets:new(wait_timers, [named_table]),
    ets:new(ji_deps, [named_table]),
    register(dron_scheduler, self()),
    {ok, []}.

handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast({schedule, Job = #job{name = JName, start_time = STime}}, State) ->
    AfterT = run_job_instances_up_to_now(
               Job,
               calendar:datetime_to_gregorian_seconds(calendar:local_time()),
               calendar:datetime_to_gregorian_seconds(STime)),
    ets:insert(start_timers, {JName, erlang:send_after(AfterT * 1000, self(),
                                                       {schedule, Job})}),
    {noreply, State};
handle_cast({unschedule, JName}, State) ->
    case ets:lookup(start_timers, JName) of
        [{JName, STRef}] -> erlang:cancel_timer(STRef),
                            ets:delete(start_timers, JName);
        []               -> ok
    end,
    case ets:lookup(schedule_timers, JName) of
        [{JName, TRef}]  -> timer:cancel(TRef),
                            ets:delete(schedule_timers, JName);
        []               -> ok
    end,
    ok = dron_db:archive_job(JName),
    {noreply, State};
handle_cast(_Request, _State) ->
    not_implemented.

handle_info({schedule, Job = #job{name = JName, frequency = Freq}}, State) ->
    {ok, TRef} = timer:apply_interval(Freq * 1000, ?MODULE,
                                      create_job_instance, [Job]),
    ets:insert(schedule_timers, {JName, TRef}),
    ets:delete(start_timers, JName),
    {noreply, State};
handle_info({succeeded, JId}, State) ->
    ok = dron_db:set_job_instance_state(JId, succeeded),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    % TODO(ionel): Move this out to consumer. (Think about satisfing various
    % resources)
    dron_scheduler ! {satisfied, JId},
    {noreply, State};
handle_info({killed, JId}, State) ->
    ok = dron_db:set_job_instance_state(JId, killed),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    {noreply, State};
handle_info({failed, JId, Reason}, State) ->
    {ok, JI = #job_instance{name = JName, worker = WName, num_retry = NumRet}} =
        dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    {ok, #job{max_retries = MaxRet}} = dron_db:get_job(JName),
    %% Check if possible to run in a new process.
    if
        NumRet < MaxRet ->
            run_instance(JI#job_instance{num_retry = NumRet + 1});
        true            ->
            ok
    end,
    {noreply, State};
handle_info({timeout, JId}, State) ->
    ok = dron_db:set_job_instance_state(JId, timeout),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    {noreply, State};
handle_info({satisfied, RId}, State) ->
    {ok, Dependants} = dron_db:get_dependants(RId),
    lists:map(fun(JId) -> satisfied_dependency(RId, JId) end, Dependants),
    {noreply, State};
handle_info({waiting_job_instance, JId, TRef, Dependencies}, State) ->
    ets:store(wait_timers, {JId, TRef}),
    ets:store(ji_deps, {JId, Dependencies}),
    {noreply, State};
handle_info({wait_timeout, JId}, State) ->
    case ets:lookup(wait_timers, JId) of
        [{JId, TRef}] -> erlang:cancel_timer(TRef),
                         ets:delete(wait_timers, JId);
        []            -> ok
    end,
    {noreply, State};
handle_info({worker_disabled, JI}, State) ->
    %% Check if it can be spawned in a new process.
    run_instance(JI),
    {noreply, State};
handle_info(_Request, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

run_job_instances_up_to_now(Job = #job{frequency = Frequency}, Now, STime) ->
    if STime < Now ->
            create_job_instance(
              Job, calendar:gregorian_seconds_to_datetime(STime)),
            run_job_instances_up_to_now(
              Job,
              calendar:datetime_to_gregorian_seconds(calendar:local_time()),
              STime + Frequency);
       true        -> STime - Now
    end.

satisfied_dependency(RId, JId) ->
    case ets:lookup(ji_deps, JId) of
        [{JId, RIds}] ->
            case lists:delete(RId, RIds) of
                []  -> case ets:lookup(wait_timers, JId) of
                           [{JId, TRef}] -> erlang:cancel_timer(TRef),
                                            ets:delete(wait_timers, JId);
                           []            -> ok 
                       end,
                       run_instance(JId),
                       ets:delete(ji_deps, JId);
                Val -> ets:insert(ji_deps, {JId, Val})
            end;
        []            ->
            ok
    end.
