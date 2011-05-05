-module(dron_pool).

-behaviour(gen_server).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2,
        code_change/3]).

-export([attach_worker/1, dettach_worker/1, dettach_workers/0,
         auto_attach_workers/0, get_workers/0]).

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

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

%-------------------------------------------------------------------------------
% Handlers
%-------------------------------------------------------------------------------

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
handle_call(get_all, _From, State = #state{workers = Workers}) ->
    {reply, dict:fetch_keys(Workers), State};

handle_cast({}, _State) ->
    not_implemented.

handle_info(_Call, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

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
