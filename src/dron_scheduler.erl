-module(dron_scheduler).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, run_instance/2, timeout_instance/1]).

-export([start_link/0, schedule/1, unschedule/1]).

-record(timers, {timers = dict:new(), jitimeout = dict:new()}).

%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

schedule(Job) ->
    gen_server:cast(?NAME, {schedule, Job}).

unschedule(Job) ->
    gen_server:cast(?NAME, {unschedule, Job}).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

run_instance(WPid, #job{name = Name, cmd_line = Cmd, timeout = Timeout}) ->
    Date = calendar:now_to_universal_time(erlang:now()),
    Worker = #worker{name = WName} = dron_pool:get_worker(),
    JId = {node(), Date},
    JobInstance = #job_instance{jid = JId, name = Name,
                                cmd_line = Cmd, state = running,
                                timeout = Timeout, run_time = erlang:now(),
                                num_retry = 1, worker = WName},
    % If the write fails then the job instance fails. Note: this should not be
    % considered a job instance failure.
    ok = dron_db:store_job_instance(JobInstance),
    dron_worker:run(WName, JobInstance),
    {ok, TRef} = timer:apply_after(Timeout, ?MODULE, timeout_instance,
                                   [JobInstance]),
    ok = dron_db:store_worker(Worker#worker{
                                used_slots = Worker#worker.used_slots + 1}),
    WPid ! {running, JId, TRef}.

timeout_instance(JI = #job_instance{jid = JId, worker = WName}) ->
    case dron_worker:kill_job_instance(WName, JId, true) of
        killed      -> ok = dron_db:store_job_instance(JI#job_instance{
                                                         state = timeout});
        not_running -> ok
    end.

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
    {ok, TRef} = timer:apply_interval(Freq, ?MODULE, run_instance,
                                      [self(), Job]),
    {noreply, #timers{timers = dict:store(JName, TRef, Timers)}};
    
handle_cast({unschedule, #job{name = JName}},
           #timers{timers = Timers}) ->
    {ok, TRef} = dict:find(JName, Timers),
    {ok, cancel} = timer:cancel(TRef),
    % TODO: Cancel the timeout timers.
    {noreply, #timers{timers = dict:erase(JName, Timers)}};

handle_cast(_Request, _State) ->
    not_implemented.

handle_info({running, JId, TRef}, State = #timers{jitimeout = JITimeout}) ->
    {noreply, State#timers{jitimeout = dict:store(JId, TRef, JITimeout)}};
handle_info({finished, JId}, State = #timers{jitimeout = JITimeout}) ->
    NewJITimeout = clear_timeout_timer(JId, JITimeout),
    ok = dron_db:set_job_instance_state(JId, finished),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    {noreply, State#timers{jitimeout = NewJITimeout}};
handle_info({killed, JId}, State = #timers{jitimeout = JITimeout}) ->
    NewJITimeout = clear_timeout_timer(JId, JITimeout),
    ok = dron_db:set_job_instance_state(JId, killed),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    {noreply, State#timers{jitimeout = NewJITimeout}};
handle_info({failed, JId, Reason}, State = #timers{jitimeout = JITimeout}) ->
    NewJITimeout = clear_timeout_timer(JId, JITimeout),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    % TODO: Handle JI failure.
    {noreply, State#timers{jitimeout = NewJITimeout}};
handle_info({timeout, JId}, State = #timers{jitimeout = JITimeout}) ->
    NewJITimeout = clear_timeout_timer(JId, JITimeout),
    {ok, #job_instance{worker = WName}} = dron_db:get_job_instance(JId),
    ok = dron_pool:release_worker_slot(WName),
    % TODO: Handle JI timeout.
    {noreply, State#timers{jitimeout = NewJITimeout}};
handle_info(_Request, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

clear_timeout_timer(JId, JITimeout) ->
    case dict:find(JId, JITimeout) of
        {ok, TRef} -> timer:cancel(TRef);
        error      -> error_logger:error_msg("~p does not have a timeout timer",
                                             [JId])
    end,
    dict:erase(JId, JITimeout).
