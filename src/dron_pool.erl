-module(dron_pool).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, add_worker/1, add_worker/2, auto_add_workers/0,
         remove_worker/1, get_worker/0, release_worker_slot/1]).

% A gb_tree of (#used_slots, [workers]) and a set of workers.
-record(workers, {workers = orddict:new(), slot_workers = gb_trees:empty()}).

%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

add_worker(WName) ->
    gen_server:call(?NAME, {add, WName, dron_config:max_slots()}).

add_worker(WName, MaxSlots) ->
    gen_server:call(?NAME, {add, WName, MaxSlots}).

% Returns a list of (worker, result).
auto_add_workers() ->
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
            Result = lists:map(fun add_worker/1, Workers),
            lists:zip(Workers, Result)
    end.
            
remove_worker(WName) ->
    gen_server:call(?NAME, {remove, WName}).

get_worker() ->
    gen_server:call(?NAME, get_worker).

release_worker_slot(WName) ->
    gen_server:call(?NAME, {release_slot, WName}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    {ok, #workers{}}.

handle_call({add, WName, Slots}, _From, State = #workers{
                                          workers = Ws,
                                          slot_workers = Sws}) ->
    case orddict:is_key(WName, Ws) of
        true  -> {reply, {error, already_added}, State};
        false -> case net_adm:ping(WName) of
                     pong -> dron_worker:start_link(WName),
                             monitor_node(WName, true),
                             SWorkers = case gb_trees:lookup(0, Sws) of
                                            {value, CurSws} -> CurSws;
                                            none            -> []
                                        end,
                             NewSws = gb_trees:enter(
                                        0, [WName | SWorkers], Sws),
                             NewW = #worker{name = WName,
                                            enabled = true,
                                            max_slots = Slots,
                                            used_slots = 0},
                             case dron_db:store_worker(NewW) of
                                 ok -> {reply, ok,
                                        State#workers{
                                          slot_workers = NewSws,
                                          workers = orddict:store(WName, NewW,
                                                                  Ws)}};
                                 Error -> {reply, Error, State}
                             end;
                     _    -> {reply, {error, no_connection}, State}
                 end
    end;
handle_call({remove, WName}, _From, State = #workers{workers = Ws,
                                                     slot_workers = Sws}) ->
       case orddict:find(WName, Ws) of
           {ok, W} -> NewWs = disable_worker(WName, Ws),
                      case dron_db:delete_worker(WName) of
                          ok -> {reply, ok,
                                 State#workers{
                                   workers = NewWs,
                                   slot_workers = evict_worker(W, Sws)}};
                          Error  -> {reply, Error, State}
                      end; 
           error   -> {reply, {error, unknown_worker}, State}
    end;
handle_call({release_slot, WName}, _From,
            State = #workers{workers = Ws, slot_workers = Sws}) ->
    case orddict:find(WName, Ws) of
        {ok, W = #worker{used_slots = USlots}} ->
                NewW = W#worker{used_slots = USlots - 1},
                {value, SlotWs} = gb_trees:lookup(USlots, Sws),
                NoSws = gb_trees:enter(USlots, lists:delete(WName, SlotWs),
                                       Sws),
                NewSlotWs = case gb_trees:lookup(USlots - 1, NoSws) of
                                {value, NSlotWs} -> [WName | NSlotWs];
                                none             -> [WName]
                            end,
                dron_db:store_worker(NewW),
                {reply, ok, State#workers{
                              workers = orddict:store(WName, NewW, Ws),
                              slot_workers = gb_trees:enter(USlots - 1,
                                                            NewSlotWs,
                                                            NoSws)}};
        error   -> {reply, {error, unknown_worker}, State}
    end;
handle_call(get_worker, _From, State = #workers{workers = Ws,
                                                slot_workers = Sws}) ->
    case get_worker(Ws, Sws) of
        {WName, NewWs, NewSws} -> {reply, orddict:fetch(WName, Ws),
                                   State#workers{
                                     workers = NewWs,
                                     slot_workers = NewSws}};
        none                   -> {reply, {error, no_workers}, State}
    end;
handle_call(Request, _From, _State) ->
    {unexpected_request, Request}.

handle_cast(_Request, _State) ->
    not_implemented.

handle_info({nodedown, WName}, State = #workers{workers = Ws,
                                                slot_workers = Sws}) ->
    error_logger:error_msg("Node ~p failed!~n", [WName]),
    {ok, W} = orddict:find(WName, Ws),
    {noreply, State#workers{workers = disable_worker(WName, Ws),
                            slot_workers = evict_worker(W, Sws)}};
handle_info(Request, _State) ->
    {unexpected_request, Request}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

% Deletes worker from the tree of (#slots, [workers]).
% It returns the same tree if the pair could not be found.
evict_worker(#worker{name = WName, used_slots = USlots}, Sws) ->
    {value, Ws} = gb_trees:lookup(USlots, Sws),
    case lists:delete(WName, Ws) of
        []   -> gb_trees:delete(USlots, Sws);
        Wls  -> gb_trees:enter(USlots, Wls, Sws)
    end.
   
get_worker(Ws, Sws) ->
    case gb_trees:is_empty(Sws) of
        true  -> none;
        false -> {Slots, [WName|Aws]} = gb_trees:smallest(Sws),
                 NewSws = add_worker_to_slot(WName, Slots + 1, Sws),
                 {ok, Worker = #worker{used_slots = UsedSlots}} =
                     orddict:find(WName, Ws),
                 NewWorker = Worker#worker{used_slots = UsedSlots + 1},
                 NewWs = orddict:store(WName, NewWorker, Ws),
                 ok = dron_db:store_worker(NewWorker),
                 case Aws of
                     [] -> {WName, NewWs, gb_trees:delete(Slots, NewSws)};
                     _  -> {WName, NewWs, gb_trees:enter(Slots, Aws, NewSws)}
                 end
    end.

add_worker_to_slot(WName, Slot, Sws) ->
    case gb_trees:lookup(Slot, Sws) of
        {value, Ws} -> gb_trees:enter(Slot, [WName | Ws], Sws);
        none        -> gb_trees:enter(Slot, [WName], Sws)
    end.

disable_worker(WName, Ws) ->
    NewWs = case orddict:find(WName, Ws) of
                {ok, Worker} -> NWs = orddict:erase(WName, Ws),
                                ok = dron_db:store_worker(Worker#worker{
                                                            enabled = false}),
                                NWs;
                error        -> error_logger:error_msg(
                                  "Worker ~p not in-memory", [WName]),
                                Ws
            end,
    % If these db writes fail then the whole worker is restarted.
    {ok, FailedJIs} = dron_db:get_job_instances_on_worker(WName),
    lists:map(fun(JI) -> dron_scheduler:worker_disabled(JI) end, FailedJIs),
    NewWs.
