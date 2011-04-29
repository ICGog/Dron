-module(master).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([start_link/0, attach_worker/1, dettach_worker/1, get_workers/0,
         send_cmd/1]).

-record(state, {workers = dict:new()}).

-record(worker, {worker,
                 task_pid = none}).

-define(NAME, {global, ?MODULE}).

 %------------------------------------------------------------------------------
 % API
 %------------------------------------------------------------------------------

 attach_worker(Worker) ->
     gen_server:call(?NAME, {attach, Worker}).

 dettach_worker(Worker) ->
     gen_server:call(?NAME, {dettach, Worker}).

 get_workers() ->
     gen_server:call(?NAME, get_all_workers).

 send_cmd(Command) ->
     gen_server:call(?NAME, {send_cmd, Command}).

 %------------------------------------------------------------------------------
 % Internal API
 %------------------------------------------------------------------------------

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
                              {reply, ok, NewState};
                      _    -> {reply, {error, no_connection}, State}
                  end
     end;
handle_call({dettach, Worker}, _From, State = #state{workers = Workers}) ->
    case dict:is_key(Worker, Workers) of
        true  -> {Reply, NewState} = dettach_node(Worker, State),
                 {reply, Reply, NewState};
        false -> {reply, {error, not_attached}, State}
    end;
handle_call(get_all_workers, _From, State = #state{workers = Workers}) ->
    {reply, dict:fetch_keys(Workers), State};
handle_call({send_cmd, Cmd}, _From, State = #state{workers = Workers}) ->
    {reply, ok, State}.

handle_cast({}, _State) ->
    not_implemented.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

dettach_node(Worker, State = #state{workers = Workers}) ->
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
                          
