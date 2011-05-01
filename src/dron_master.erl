-module(dron_master).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([start_link/0, attach_worker/1, dettach_worker/1, dettach_all_workers/0,
         auto_attach_workers/0, get_workers/0, send_cmd/1]).

-record(state, {workers = dict:new()}).

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

send_cmd(Command) ->
    gen_server:call(?NAME, {send_cmd, Command}).

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
                     pong -> NewWs = dict:store(W, #worker{worker = W}, Ws),
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
handle_call({send_cmd, Cmd}, _From, State = #state{workers = Workers}) ->
    Worker = pick_worker(Workers),
    {ok, Stdout} = worker:run_cmd(Cmd),
    io:format("Command: ~s~nOutput: ~s", [Cmd, Stdout]),
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
    list:nth(Nth, WList).
