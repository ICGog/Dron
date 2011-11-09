-module(dron_worker).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([start_link/1, run/3, kill_job_instance/2, kill_job_instance/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, run_job_instance/1]).

% A dict of (JId, Pid) and a dict of (Pid, JId).
-record(jipids, {jipids = dict:new(), pidjis = dict:new(),
                jitimeout = dict:new()}).

%-------------------------------------------------------------------------------

start_link(WName) ->
    gen_server:start_link({global, WName}, ?MODULE, [], []).

run(WName, JobInstance, Timeout) ->
    gen_server:cast({global, WName}, {run, JobInstance, Timeout}).

%% @doc Kills a job instance.
kill_job_instance(WName, JId) ->
    kill_job_instance(WName, JId, false).

kill_job_instance(WName, JId, Timeout) ->
    gen_server:cast({global, WName}, {kill, JId, Timeout}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    {ok, #jipids{}}.

handle_call(_Request, _From, _State) ->
    unexpected_request.

handle_cast({run, JI = #job_instance{jid = JId, worker = WName}, Timeout},
            State = #jipids{jipids = JIPids, pidjis = PidJIs,
                           jitimeout = JITimeout}) ->
    {ok, TRef} = timer:apply_after(Timeout, ?MODULE, kill_job_instance,
                                   [WName, JId, true]),
    NewJITimeout = dict:store(JId, TRef, JITimeout),
    JIPid = spawn_link(dron_worker, run_job_instance, [JI]),
    NewJIPids = dict:store(JId, JIPid, JIPids),
    NewPidJIs = dict:store(JIPid, JId, PidJIs),
    JIPid ! {self(), start},
    {noreply, State#jipids{jipids = NewJIPids, pidjis = NewPidJIs,
                          jitimeout = NewJITimeout}};
handle_cast({kill, JId, Timeout}, State = #jipids{jipids = JIPids,
                                                 pidjis = PidJIs,
                                                 jitimeout = JITimeout}) ->
    Reason = case Timeout of
                 false -> killed;
                 true  -> timeout
             end,
    case dict:find(JId, JIPids) of
        {ok, Pid} -> exit(Pid, {JId, Reason}),
                     {noreply,
                      State#jipids{jipids = dict:erase(JId, JIPids),
                                   pidjis = dict:erase(Pid, PidJIs),
                                   jitimeout = clear_timeout(JId, JITimeout)}};
        error     -> {noreply, State}
    end;
handle_cast(_Request, _State) ->
    unexpected_request.

handle_info({JId, JPid, ok}, State = #jipids{jipids = JIPids,
                                            pidjis = PidJIs,
                                            jitimeout = JITimeout}) ->
    %% Notifies the scheduler that a job instance has finished.
    dron_scheduler ! {finished, JId},
    {noreply, State#jipids{jipids = dict:erase(JId, JIPids),
                          pidjis = dict:erase(JPid, PidJIs),
                          jitimeout = clear_timeout(JId, JITimeout)}};
handle_info({'EXIT', _Pid, normal}, State) ->
    %% A job instance finished successfully.
    {noreply, State};
handle_info({'EXIT', _Pid, {JId, Reason}}, State) ->
    error_logger:info_msg("~p has been killed", [JId]),
    %% Notify the scheduler why the job instance was killed.
    %% (timeout | killed).
    dron_scheduler ! {Reason, JId},
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, State = #jipids{jipids = JIPids,
                                                  pidjis = PidJIs,
                                                  jitimeout = JITimeout}) ->
    error_logger:info_msg("Job instance exited: ~p ~p", [Pid, Reason]),
    case dict:find(Pid, PidJIs) of
        {ok, JId} -> dron_scheduler ! {failed, JId, Reason},
                     {noreply,
                      State#jipids{jipids = dict:erase(JId, JIPids),
                                   pidjis = dict:erase(Pid, PidJIs),
                                   jitimeout = clear_timeout(JId, JITimeout)}};
        error     -> {noreply, State}
    end;
handle_info(_Info, _State) ->
    unexpected_request.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

run_job_instance(JI = #job_instance{jid = JId, name = Name, cmd_line = Cmd}) ->
    error_logger:info_msg("~p", [JI]),
    {_, {{Y, M, D}, {H, Min, Sec}}} = JId,
    WPid = receive
               {Pid, start} -> error_logger:info_msg(
                                 "Started job instance ~p~n", [JId]),
                               Pid
           after 10000 ->
                   exit(no_start_timeout)
           end,
    Output = os:cmd(Cmd),
    error_logger:info_msg("Output:~s~n", [Output]),
    FileName = io_lib:format("~s_~p-~p-~p-~p:~p:~p", [Name, Y, M, D, H, Min,
                                                      Sec]),
    file:write_file(FileName, io_lib:fwrite("~s", [Output]), [write]),
    WPid ! {JId, self(), ok}.

clear_timeout(JId, JITimeout) ->
    case dict:find(JId, JITimeout) of
        {ok, TRef} -> timer:cancel(TRef);
        error      -> error_logger:error_msg("~p does not have a timeout timer",
                                             [JId])
    end,
    dict:erase(JId, JITimeout).
