-module(dron_pool).

-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2,
        code_change/3]).

-export([pspawn/3, pspawn_link/3, pspawn/4, start_link/0, attach_worker/1,
         dettach_worker/1, dettach_workers/0, auto_attach_workers/0, 
         get_workers/0, kill_worker/1, kill_workers/0]).

-record(state, {workers = dict:new(),
                free_workers = dict:new()}).

-record(worker, {worker,
                 task_pid = none}).

-define(NAME, {global, ?MODULE}).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

attach_worker(Worker) ->
    gen_server:call(?NAME, {attach, Worker}).

dettach_worker(Worker) ->
    gen_server:call(?NAME, {dettach, Worker}).

dettach_workers() ->
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
    gen_server:call(?NAME, get_workers).

kill_worker(Worker) ->
    gen_server:call(?NAME, {kill, Worker}).

kill_workers() ->
    gen_server:call(?NAME, kill_workers).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

pspawn(Module, Function, Args) ->
    pspawn(Module, Function, Args, false).

pspawn_link(Module, Function, Args) ->
    pspawn(Module, Function, Args, {true, self()}).

pspawn(Module, Function, Args, Link) ->
    gen_server:call(?NAME, {spawn, {Module, Function, Args}, Link}).

%-------------------------------------------------------------------------------
% Handlers
%-------------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

handle_call({attach, W}, _From, State = #state{workers = Ws,
                                               free_workers = FWs}) ->
    case dict:is_key(W, Ws) or dict:is_key(W, FWS) of
        true  -> {reply, {error, already_attached}, State};
        false -> case net_adm:ping(W) of
                     pong -> NewWs = dict:store(W, #worker{name = W}, FWs),
                             NewState = State#state{free_workers = NewWs},
                             error_logger:info_msg("Worker attached: ~p", [W]),
                             {reply, ok, NewState};
                     _    -> {reply, {error, no_connection}, State}
                 end
    end;
handle_call({dettach, W}, _From, State = #state{workers = Ws,
                                                free_workers = FWS}) ->
    case dict:is_key(W, FWs) or dict:is_key(W, Ws) of
        true  -> {Reply, NewState} = dettach_worker(W, State),
                 error_logger:info_msg("Worker dettached: ~p", [W]),
                 {reply, Reply, NewState};
        false -> {reply, {error, not_attached}, State}
    end;
handle_call(get_workers, _From, State = #state{workers = Ws
                                               free_workers = FWs}) ->
    {reply, lists:append(dict:fetch_keys(Ws), 
                         dict:fetch_keys(FWs), State};
handle_call({kill, Worker}, _From, State) ->
    {Reply, NewState} = kill_worker(Worker, State),
    {reply, Reply, NewState};
handle_call(kill_workers, _From, State = #state{workers = Ws,
                                                free_workers = FWs}) ->
    NewState = dict:fold(fun (W, _, CurState) ->
                                 quick_kill_worker(W, CurState)
                         end, State, dict:fetch_keys(Ws)),
    FinalState = dict:fold(fun (W, _, CurState) ->
                                   quick_kill_worker(W, CurState)
                           end, NewState, dict:fetch_keys(FWs)),
    {reply, ok, FinalState};
handle_call({spawn, {Module, Function, Args} = MFA, Link}, From,
            State = #state{workers = Ws, free_workers = FWs}) ->
    % TODO(ionel): Queue the requests when there are no workers.
    Worker = pick_worker(FWs),
    Pid = spawn(Worker, ?MODULE, worker_init, [MFA, Link, self()]),
    Monitor = erlang:monitor(process, Pid),
    receive
        {started, Pid} -> 
            NewFWs = dict:erase(Worker, FWs),
            NewWs = dict:store(#worker{worker = Worker,
                                       task_pid = Pid},
                               Ws),
            {reply, State#state{workers = NewWs, free_workers = NewFWs}};
        {'DOWN', Monitor, process, Pid, Reason} ->
            case Link of {true, Caller} -> exit(Caller, Reason);
                         false          -> ok
            end,
            {reply, {error, could_not_spawn}, State}
    end.
        
handle_cast({}, _State) ->
    not_implemented.

handle_info({'DOWN', _, process, Pid, noconnection}, State) ->
    error_logger:error_msg("No connection to ~p", [node(Pid)]),
    % What should we do when there is no connection?
    {noreply, State},
handle_info({'DOWN', _, process, Pid, noproc}, State) ->
    error_logger:error_msg("Failed to start on ~p", [node(Pid)]),
    % What should we do when the proc fails to start?
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

dettach_worker(Worker, State = #state{workers = Ws,
                                      free_workers = FWs}) ->
    Reply =
        case dict:is_key(Worker, FWs) of
            true  -> dict:erase(Worker, FWs),
                     ok;
            false -> case dict:fetch(Worker, Ws) of
                         #worker{task_pid = none}    ->
                             ok;
                         #worker{task_pid = TaskPid} ->
                             error_logger:info_msg("Killed task on worker ~p",
                                                   [node(TaskPid)])
                                 exit(TaskPid, kill),
                             receive {'DOWN', _, process, TaskPid, _} -> ok
                             after 10000 -> {error,
                                             timed_out_waiting_task_pid_down}
                             end
                     end,
        end,
    {Reply, State#state{workers = Ws, free_workers = FWs}}.
        
kill_worker(Worker, State = #state{workers = Ws}) ->
    case dict:is_key(Worker, Ws) of
        true  -> error_logger:info_msg("Killing node ~p", [Worker]),
                 rpc:call(Worker, init, stop, []),
                 receive {nodedown, Worker} ->
                         {ok, State#state{workers = dict:erase(Worker, Ws)}}
                 after 10000 ->
                         monitor_node(Worker, false),
                         {{error, timeout_on_killing}, State}
                 end;
        false -> {{error, nothing_running}, State}
    end.
       
quick_kill_worker(Worker, State = #state{workers = Ws,
                                         free_workers = FWs}) ->
    monitor_node(Worker, false),
    rpc:cast(Worker, erlang, halt, []),
    State#state{workers = dict:erase(Worker, Ws),
                free_workers = dict:erase(Worker, FWs)}.
           
pick_worker(Workers) ->
    WList = dict:to_list(Workers),
    Nth = random:uniform(length(WList)),
    {_, Worker} = lists:nth(Nth, WList),
    Worker.
