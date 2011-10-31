-module(dron_pool).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start/0, add_worker/1, auto_add_workers/0, remove_worker/1,
         get_worker/0]).

% A gb_tree of (#used_slots, [workers]) and a set of workers.
-record(workers, {workers = sets:new(), slot_workers = gb_trees:new()}).

%-------------------------------------------------------------------------------

start() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

add_worker(Worker) ->
    gen_server:call(?NAME, {add, Worker}).

% Returns a list of (worker, result).
auto_add_workers() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    case os:getenv("DRON_WORKER_NODES") of
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
            
remove_worker(Worker) ->
    gen_server:call(?NAME, {remove, Worker}).

get_worker() ->
    gen_server:call(?NAME, {get_worker}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    {ok, #workers{}}.

handle_call({add, Worker}, _From, State = #workers{workers = Ws,
                                                  slot_workers = Sws}) ->
    case sets:is_element(Worker, Ws) of
        true  -> {reply, {error, already_added}, State};
        false -> case net_adm:ping(Worker) of
                     pong -> dron_worker:start({global, Worker}),
                             SWorkers = case gb_trees:lookup(0, Sws) of
                                            {value, CurSws} -> CurSws;
                                            none            -> []
                                        end,
                             NewSws = gb_trees:enter(
                                        0, [Worker | SWorkers], Sws),
                             {reply, ok,
                              State#workers{
                                slot_workers = NewSws,
                                workers = sets:add_element(Worker, Ws)}};
                     _    -> {reply, {error, no_connection}, State}
                 end
    end;
handle_call({remove, Worker}, _From, State = #workers{workers = Ws,
                                                     slot_workers = Sws}) ->
    case sets:is_element(Worker, Ws) of
        true  -> {reply, ok, State#workers{
                               workers = sets:del_element(Worker, Ws),
                               slot_workers =
                                   delete_worker(Worker, Sws,
                                                 gb_trees:iterator(Sws))}};
        false -> {reply, {error, unknown_worker}, State}
    end;
handle_call({get_worker}, _From, State = #workers{slot_workers = Sws}) ->
    case get_worker(Sws) of
        {Worker, NewSws} -> {reply, Worker, State#workers{
                                              slot_workers = NewSws}};
        none             -> {reply, {error, no_workers}, State}
    end;
handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast(_Request, _State) ->
    not_implemented.

handle_info(_Request, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

% Find worker in the tree of (#slots, [workers]).
% It returns the same tree if the pair could not be found.
delete_worker(W, Sws, Iter) ->
    case gb_trees:next(Iter) of
        {Key, Value, NewIter} ->
            case delete_worker(W, Value) of
                []   -> gb_trees:delete(Key, Sws);
                none -> delete_worker(W, Sws, NewIter);
                Wls  -> gb_trees:enter(Key, Wls, Sws)
             end;
        none -> Sws
    end.

% Deletes a worker from a list if it can find it.
delete_worker(_, []) ->
    none;
delete_worker(W, [W|Wls]) ->
    Wls;
delete_worker(W, [_|Wls]) ->
    [W|delete_worker(W, Wls)].

get_worker(Sws) ->
    case gb_trees:is_empty(Sws) of
        true  -> none;
        false -> {Slots, [W|Aws]} = gb_trees:smallest(Sws),
                 NewSws = add_worker_to_slot(W, Slots + 1, Sws),
                 case Aws of
                     [] -> gb_trees:delete(Slots, NewSws);
                     _  -> gb_trees:enter(Slots, Aws, NewSws)
                 end
    end.

add_worker_to_slot(W, Slot, Sws) ->
    case gb_trees:lookup(Slot, Sws) of
        {value, Ws} -> gb_trees:enter(Slot, [W | Ws]);
        none        -> gb_trees:enter(Slot, [W])
    end.
