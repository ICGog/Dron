-module(dron_master).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([start_link/0, attach_worker/1, dettach_worker/1, dettach_all_workers/0,
         auto_attach_workers/0, get_workers/0, add_job/1, add_job/2]).

-include("dron.hrl").

-record(state, {workers = dict:new()}).

-record(worker, {name,
                 task_pid = none}).

-define(NAME, {global, ?MODULE}).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

attach_worker(Worker) ->
    gen_server:call(?NAME, {attach, Worker}).

dettach_worker(Worker) ->
    gen_server:call(?NAME, {dettach, Worker}).

dettach_all_workers() ->
    Workers = get_workers(),
    Result = lists:map(fun dettach_worker/1, Workers),
    lists:zip(Workers, Result).

auto_attach_workers() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    case os:getenv("DRON_WORKERS") of
        false ->
            [];
        WorkersEnv ->
            Workers = lists:map(
                        fun(Worker) ->
                                list_to_atom(
                                  case lists:member($@, Worker) of
                                      true  -> Worker;
                                      false -> Worker ++ "@" ++ Host
                                  end)
                        end, string:tokens(WorkersEnv, " \n\t")),
            lists:zip(Workers, lists:map(fun attach_worker/1, Workers))
    end.
    
get_workers() ->
    gen_server:call(?NAME, get_all_workers).

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

handle_call({attach, W}, _From, State = #state{workers = Ws}) ->
    case dict:is_key(W, Ws) of
        true  -> {reply, {error, already_attached}, State};
        false -> case net_adm:ping(W) of
                     pong -> NewWs = dict:store(W, #worker{name = W}, Ws),
                             NewState = State#state{workers = NewWs},
                             error_logger:info_msg("Worker attached: ~p", [W]),
                             {reply, ok, NewState};
                     _    -> {reply, {error, no_connection}, State}
                 end
    end;
handle_call({dettach, W}, _From, State = #state{workers = Ws}) ->
    case dict:is_key(W, Ws) of
        true  -> {Reply, NewState} = dettach_worker(W, State),
                 error_logger:info_msg("Worker dettached: ~p", [W]),
                 {reply, Reply, NewState};
        false -> {reply, {error, not_attached}, State}
    end;
handle_call(get_all_workers, _From, State = #state{workers = Workers}) ->
    {reply, dict:fetch_keys(Workers), State};
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

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

dettach_worker(Worker, State = #state{workers = Workers}) ->
    Reply =
        case dict:fetch(Worker, Workers) of
            #worker{task_pid = none}    ->
                ok;
            #worker{task_pid = TaskPid} ->
                exit(TaskPid, kill),
                receive {'DOWN', _, process, TaskPid, _} -> ok
                after 10000 -> {error, timed_out_waiting_task_pid_down}
                end
        end,
    {Reply, State#state{workers = dict:erase(Worker, Workers)}}.
                          
pick_worker(Workers) ->
    WList = dict:to_list(Workers),
    Nth = random:uniform(length(WList)),
    {_, Worker} = lists:nth(Nth, WList),
    Worker.
