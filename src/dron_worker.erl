-module(dron_worker).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([start/1, stop/1, start_link/1, run/3, kill_job_instance/2,
         kill_job_instance/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, run_job_instance/1]).

% A dict of (JId, Pid) and a dict of (Pid, JId).
-record(wstate, {jipids = dict:new(), pidjis = dict:new(),
                 jitimeout = dict:new()}).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start a worker server.
%%
%% @spec start(WorkerName) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%------------------------------------------------------------------------------
start(WName) ->
  gen_server:start({global, WName}, ?MODULE, [], []).

start_link(WName) ->
  gen_server:start_link({global, WName}, ?MODULE, [], []).

stop(WName) ->
  gen_server:call({global, WName}, stop).

%%------------------------------------------------------------------------------
%% @doc
%% Run a job instance on a given worker. The call does not block.
%%
%% @spec run(WorkerName, JobInstance, Timeout) -> ok
%% @end
%%------------------------------------------------------------------------------
run(WName, JobInstance, Timeout) ->
  error_logger:info_msg("RUUUUUN"),
  gen_server:cast({global, WName}, {run, JobInstance, Timeout}).

%%------------------------------------------------------------------------------
%% @doc
%% Kills a job instance. The call does not block.
%%
%% @spec kill_job_instance(WorkerName, JId) -> ok
%% @end
%%------------------------------------------------------------------------------
kill_job_instance(WName, JId) ->
  kill_job_instance(WName, JId, false).

%%------------------------------------------------------------------------------
%% @doc
%% Kills a job instance. Timeout = true if the job is killed because of timeout.
%% The call does not block.
%%
%% @spec kill_job_instance(WorkerName, JId, Timeout) -> ok
%% @end
%%------------------------------------------------------------------------------
kill_job_instance(WName, JId, Timeout) ->
  gen_server:cast({global, WName}, {kill, JId, Timeout}).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
  process_flag(trap_exit, true),
  {ok, #wstate{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  error_logger:info_msg("Shutting down worker ~p", [node()]),
  {stop, shutdown, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({run, JI = #job_instance{jid = JId, worker = WName}, Timeout},
            State = #wstate{jipids = JIPids, pidjis = PidJIs, 
                            jitimeout = JITimeout}) ->
  error_logger:info_msg("RUUUUUN22222"),
  {ok, TRef} = timer:apply_after(Timeout * 1000, ?MODULE, kill_job_instance,
                                 [WName, JId, true]),
  JIPid = spawn_link(dron_worker, run_job_instance, [JI]),
  JIPid ! {self(), start},
  {noreply, State#wstate{jipids = dict:store(JId, JIPid, JIPids),
                         pidjis = dict:store(JIPid, JId, PidJIs),
                         jitimeout = dict:store(JId, TRef, JITimeout)}};
handle_cast({kill, JId, TimeoutReason},
            State = #wstate{jipids = JIPids, pidjis = PidJIs,
                            jitimeout = JITimeout}) ->
  Reason = case TimeoutReason of
             false -> killed;
             true  -> timeout
           end,
  case dict:find(JId, JIPids) of
    {ok, Pid} -> exit(Pid, {JId, Reason}),
                 {noreply,
                 State#wstate{jipids = dict:erase(JId, JIPids),
                              pidjis = dict:erase(Pid, PidJIs),
                              jitimeout = clear_timeout(JId, JITimeout)}};
    error     -> error_logger:error_msg(
                   "Job Instance does not exist ~p", [JId]), 
                 {noreply, State}
  end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({JId, JPid, ok}, State = #wstate{jipids = JIPids,
                                             pidjis = PidJIs,
                                             jitimeout = JITimeout}) ->
  %% Notifies the scheduler that a job instance has finished.
  publish_state(JId, <<"succeeded">>),
  {noreply, State#wstate{jipids = dict:erase(JId, JIPids),
                         pidjis = dict:erase(JPid, PidJIs),
                         jitimeout = clear_timeout(JId, JITimeout)}};
handle_info({'EXIT', _Pid, normal}, State) ->
  %% A job instance finished successfully. The state is already updated when
  %% the worker is informed that the job instance finished.
  {noreply, State};
handle_info({'EXIT', _Pid, {JId, Reason}}, State) ->
  %% Notify the scheduler why the job instance was killed.
  %% (timeout | killed).
  publish_state(JId, list_to_binary(atom_to_list(Reason))),
  {noreply, State};
handle_info({'EXIT', Pid, Reason}, State = #wstate{jipids = JIPids,
                                                   pidjis = PidJIs,
                                                   jitimeout = JITimeout}) ->
  error_logger:info_msg("Job instance exited: ~p ~p", [Pid, Reason]),
  case dict:find(Pid, PidJIs) of
    {ok, JId} -> publish_state(JId, <<"failed">>, atom_to_list(Reason)),
                 {noreply,
                  State#wstate{jipids = dict:erase(JId, JIPids),
                               pidjis = dict:erase(Pid, PidJIs),
                               jitimeout = clear_timeout(JId, JITimeout)}};
    error     -> error_logger:error_msg(
                   "Job Instance with PID ~p does not exist", [Pid]), 
                 {noreply, State}
  end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Run a job instance. It writes the output in a file.
%%
%% @spec run_job_instance(JobInstance) -> ok
%% @end
%%------------------------------------------------------------------------------
run_job_instance(#job_instance{jid = JId, name = Name, cmd_line = Cmd}) ->
  {_, {{Y, M, D}, {H, Min, Sec}}} = JId,
  WPid = receive
           {Pid, start} -> Pid
           after 10000  -> exit(no_start_timeout)
         end,
  FileName = io_lib:format("~s_~p-~p-~p-~p:~p:~p",
                           [Name, Y, M, D, H, Min, Sec]),
  file:write_file(FileName, io_lib:fwrite("~s", [os:cmd(Cmd)]), [write]),
  WPid ! {JId, self(), ok}.

clear_timeout(JId, JITimeout) ->
  case dict:find(JId, JITimeout) of
    {ok, TRef} -> timer:cancel(TRef);
    error      -> error_logger:error_msg(
                    "~p does not have a timeout timer", [JId])
  end,
  dict:erase(JId, JITimeout).

publish_state(JId, State) ->
  dron_pubsub:publish_message(
    dron_config:dron_exchange(), <<"">>,
    list_to_binary(mochijson2:encode({struct,
                                      [{<<"job_instance">>,
                                        job_instance_id_to_binary(JId)},
                                       {<<"state">>, State}]}))).

publish_state(JId, State, Reason) ->
  dron_pubsub:publish_message(
    dron_config:dron_exchange(), <<"">>,
    list_to_binary(mochijson2:encode({struct,
                                      [{<<"job_instance">>,
                                        job_instance_id_to_binary(JId)},
                                       {<<"state">>, State},
                                       {<<"reason">>, Reason}]}))).

job_instance_id_to_binary({Host, {{Year, Month, Day}, {Hour, Min, Sec}}}) ->
  [list_to_binary(Host), Year, Month, Day, Hour, Min, Sec].
