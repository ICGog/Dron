-module(dron_master).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([start_link/0, add_job/1, add_job/2]).

-include("dron.hrl").

-record(state, {workers = dict:new()}).

-record(worker, {name,
                 task_pid = none}).

-define(NAME, {global, ?MODULE}).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

add_job(Job) ->
    gen_server:call(?NAME, {add_job, Job}).

add_job(Id, Cmd) ->
    gen_server:call(?NAME, {add_job, #dron_job{id = Id,
                                               state = waiting,
                                               started_on = not_yet,
                                               ended_on = not_yet,
                                               cmd = Cmd}}).
                                               
%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

%------------------------------------------------------------------------------
% Handlers
%------------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call({add_job, Job}, _From, State = #state{workers = Workers}) ->
    case dron_mnesia:put_job(Job) of
        {aborted, Reason} -> {reply, {aborted, Reason}, State};
        _                 -> {reply, ok, State}
    end,
    Worker = pick_worker(Workers),
    spawn_link(Worker#worker.name, dron_worker, run_cmd, [Job#dron_job.cmd, self()]),
    receive 
        {ok, Output}    ->
            io:format("OK: ~p", [Output]);
        {error, Output} ->
            io:format("ERROR: ~p", [Output])
    end,
    {reply, ok, State}.

handle_cast({}, _State) ->
    not_implemented.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

