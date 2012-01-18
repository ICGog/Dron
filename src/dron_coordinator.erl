-module(dron_coordinator).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_leader).

-export([init/1, handle_call/4, handle_cast/3, handle_info/2,
         handle_leader_call/4, handle_leader_cast/3, handle_DOWN/3,
         elected/3, surrendered/3, from_leader/3, code_change/4, terminate/2]).

-export([start_link/2, schedule/1, unschedule/1]).

-export([job_instance_succeeded/1, job_instance_failed/2,
         job_instance_timeout/1, job_instance_killed/1,
         dependency_satisfied/1, worker_disabled/1]).

-record(state, {leader, schedulers, nodes, num_nodes}).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts the coordinator on a list of nodes. One node will be elected as
%% leader.
%%
%% @spec start_link(MasterNodes, SchedulerNodes) -> ok
%% @end
%%------------------------------------------------------------------------------
start_link(MasterNodes, SchedulerNodes) ->
    gen_leader:start_link(?MODULE, MasterNodes, [], ?MODULE,
                          [SchedulerNodes], []).

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

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([SchedulerNodes]) ->
    {ok, #state{schedulers = SchedulerNodes}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
elected(State, _Election, undefined) ->
    error_logger:info_msg("~p elected as master", [node()]),
    dron_pool:start_link(),
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
handle_leader_call(Request, _From, State, _Election) ->
    error_logger:error_msg("Unexpected leader call ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_leader_cast({schedule, Job = #job{name = Name}},
                   State = #state{schedulers = Schedulers}, _Election) ->
    error_logger:info_msg("~p ~p ~p", [Name, Schedulers, dron_hash:hash(Name,
                                                                        Schedulers)]),
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
handle_leader_cast({worker_disabled, JI}, State, _Election) ->
    %% TODO(ionel): Do the call to the appropriate scheduler.
    {ok, State};
handle_leader_cast(_Request, State, _Election) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
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
