-module(dron_scheduler).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, run_instance_from_job/1, run_instance/1]).

-export([start_link/0, schedule/1, unschedule/1]).

-record(timers, {timers = dict:new()}).

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

run_instance_from_job(#job{name = Name, cmd_line = Cmd,
                                 timeout = Timeout}) ->
    Date = calendar:now_to_universal_time(erlang:now()),
    run_instance(#job_instance{jid = {node(), Date}, name = Name,
                               cmd_line = Cmd, state = running,
                               timeout = Timeout, run_time = erlang:now(),
                               num_retry = 0, worker = undefined}).

run_instance(NoWorkerJI = #job_instance{timeout = Timeout}) ->
    #worker{name = WName} = dron_pool:get_worker(),
    JI = NoWorkerJI#job_instance{worker = WName},
    % If the write fails then the job instance fails. Note: this should not be
    % considered a job instance failure.
    ok = dron_db:store_job_instance(JI),
    dron_worker:run(WName, JI, Timeout).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    register(dron_scheduler, self()),
    {ok, #timers{}}.

handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast({schedule, Job = #job{name = JName, start_time = STime,
                                  frequency = Freq}},
            #timers{timers = Timers}) ->
    {ok, TRef} = timer:apply_interval(Freq, ?MODULE, run_instance_from_job,
                                      [Job]),
    {noreply, #timers{timers = dict:store(JName, TRef, Timers)}};
    
handle_cast({unschedule, JName}, #timers{timers = Timers}) ->
    {ok, TRef} = dict:find(JName, Timers),
    {ok, cancel} = timer:cancel(TRef),
    ok = dron_db:archive_job(JName),
    {noreply, #timers{timers = dict:erase(JName, Timers)}};

handle_cast(_Request, _State) ->
    not_implemented.

handle_info({succeeded, JId}, State) ->
    ok = dron_db:set_job_instance_state(JId, succeeded),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
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
