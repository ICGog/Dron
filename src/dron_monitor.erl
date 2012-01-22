-module(dron_monitor).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([new_scheduler_leader/2, start_new_workers/3, store_new_workers/2,
        start_new_scheduler/3, add_workers/2, add_scheduler/3,
        remove_scheduler/2, remove_workers/1, assign_workers/1,
        balance_workers/1]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts a list of new workers. Returns a list containing the workers that have
%% been successfully started.
%%
%% @spec start_new_workers(Schedulers, SchedulerName, Workers) ->
%%    NewWorkers | {error, unknown_scheduler}
%% @end
%%------------------------------------------------------------------------------
start_new_workers(Scheduler, SName, Workers) ->
    case lists:member(SName, Scheduler) of
        false -> {error, unknown_scheduler};
        true  -> start_new_workers(SName, Workers)
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start_new_workers(SName, Workers) ->
    NewWs = get_new_workers(Workers),
    OkWs = lists:filter(
             fun(W) -> case rpc:call(SName, dron_pool, add_worker, [W]) of
                           ok  -> true;
                           Ret -> error_logger:error_msg(
                                    "Got ~p while starting ~p", [W, Ret]),
                                  false
                       end
             end, NewWs),
    error_logger:info_msg("New workers ~p added to ~p", [OkWs, SName]),
    store_new_workers(SName, OkWs),
    OkWs.
    

%%------------------------------------------------------------------------------
%% @doc
%% Store the new workers. They must be brand new.
%%
%% @spec store_workers(SchedulerName, Workers) -> ok
%% @end
%%------------------------------------------------------------------------------
store_new_workers(SName, Ws) ->
    lists:map(fun(W) -> ets:insert(worker_scheduler, {W, SName}) end, Ws),    
    ets:insert(scheduler_assig,
               {SName, lists:append(get_scheduler_workers(SName), Ws)}),
    ok.

%%------------------------------------------------------------------------------
%% @doc
%% Starts a new scheduler together with its workers. The workers that are used
%% by other schedulers are not added.
%%
%% @spec start_new_workers(Schedulers, SchedulerName, Workers) ->
%%    NewWorkers | {error, Reason} | {error, already_added}
%% @end
%%------------------------------------------------------------------------------
start_new_scheduler(Schedulers, SName, Workers) ->
    case lists:member(SName, Schedulers) of
        true  -> {error, already_added};
%% TODO(ionel): This only works when the scheduler master is made only of
%% one node. Otherwise, dron_scheduler:start may elect a different leader. Thus,
%% one may add workers to an unsuitable slave scheduler.
        false -> case rpc:call(SName, dron_scheduler,
                               start, [[SName], node()]) of
                     {ok, _Pid} -> ets:insert(scheduler_load,
                                          {SName, {0, calendar:local_time()}}),
                                   timer:sleep(200),
                                   start_new_workers(SName,
                                                     get_new_workers(Workers));
                     Reason     -> {error, Reason}
                 end
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Adds new workers to a scheduler. The workers already used are ignored.
%%
%% @spec add_workers(SchedulerName, Workers) -> NewWorkers
%% @end
%%------------------------------------------------------------------------------
add_workers(SName, Workers) ->
    NewWs = get_new_workers(Workers),
    store_new_workers(SName, NewWs),
    NewWs.

%%------------------------------------------------------------------------------
%% @doc
%% Adds a new scheduler. The workers already used are ignored.
%%
%% @spec add_scheduler(Schedulers, SchedulerName, Workers) ->
%%    AddedWorkers | {error, no_workers} | {error, already_exists}
%% @end
%%------------------------------------------------------------------------------
add_scheduler(Schedulers, SName, Workers) ->
    case lists:member(SName, Schedulers) of
        true  -> {error, already_added};
        false -> NewWs = get_new_workers(Workers),
                 case NewWs of
                     [] -> {error, no_workers};
                     _  -> ets:insert(scheduler_load,
                             {SName, {0, calendar:local_time()}}),
                           store_new_workers(SName, NewWs),
                           NewWs
                 end
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Removes a scheduler. Its workers do not get reassigned.
%%
%% @spec remove_scheduler(Schedulers, SchedulerName) ->
%%    ok | {error, unknown_scheduler}
%% @end
%%------------------------------------------------------------------------------
remove_scheduler(Schedulers, SName) ->
    case lists:member(SName, Schedulers) of
        false -> {error, unknown_scheduler};
        true  -> ets:delete(scheduler_load, SName),
                 Ws = get_scheduler_workers(SName),
                 ets:delete(scheduler_assig, SName),
                 lists:map(fun(W) ->
                                   ets:delete(worker_scheduler, W)
                           end, Ws),
                 ok
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Removes a list of workers.
%%
%% @spec remove_workers(Workers) -> RemovedWorkers
%% @end
%%------------------------------------------------------------------------------
remove_workers(Workers) ->
    lists:filter(fun(W) ->
                      case ets:lookup(worker_scheduler, W) of
                          [{_, SName}] -> remove_worker_scheduler(SName, W),
                                          true;
                          []           -> false
                      end
              end, Workers).

%%------------------------------------------------------------------------------
%% @doc
%% Reassings the workers of a scheduler to another one.
%%
%% @spec new_scheduler_leader(OldScheduler, NewScheduler) -> ok
%% @end
%%------------------------------------------------------------------------------
new_scheduler_leader(OldSched, NewSched) ->
    Ws = get_scheduler_workers(OldSched),
    ets:delete(scheduler_assig, OldSched),
    ets:delete(scheduler_load, OldSched),
    lists:map(fun(Worker) ->
                      ets:insert(worker_scheduler, {Worker, NewSched})
              end, Ws),
    ets:insert(scheduler_assig, {NewSched, Ws}),
    ets:insert(scheduler_load, {NewSched, {0, calendar:local_time()}}),
    ok.

%%------------------------------------------------------------------------------
%% @doc
%% Assigns workers stored in environment variable to the given list of
%% schedulers.
%%
%% @spec assign_workers(Schedulers) -> [{Scheduler, Workers}...]
%% @end
%%------------------------------------------------------------------------------
assign_workers(Schedulers) ->
    Workers = lists:map(
                fun(Worker) ->
                        case net_adm:ping(Worker) of
                            pong -> {ok, Worker};
                            _    -> {unavailable, Worker}
                        end
                end, dron_config:worker_nodes()),
    error_logger:info_msg("Pinging workers: ~p~n", [Workers]),
    {_, OkWorkers} = lists:unzip(
                       lists:filter(fun(WRes) ->
                                            case WRes of
                                                {ok, _Worker} -> true;
                                                _             -> false
                                            end
                                    end, Workers)),    
    NumWorkers = length(OkWorkers),
    NumSchedulers = length(Schedulers),
    WPerScheduler = NumWorkers div NumSchedulers,
    ExtraWorkers = NumWorkers rem NumSchedulers,
    assign_workers(OkWorkers, Schedulers, WPerScheduler, ExtraWorkers, []).

%% TODO(ionel): Finish this method and call it from time to time. There
%% should also be another one to check for scheduler failure.
%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec balance_workers(Loads)
%% @end
%%------------------------------------------------------------------------------
balance_workers([]) ->
    ok;
balance_workers([{Load, {SName, Time}} | Rest]) ->
    TimeDiff = calendar:time_difference(Time, calendar:local_time()),
    Expired = TimeDiff > dron_config:scheduler_load_timeout(),
    if
        Expired -> ok; %% TODO(ionel): handle load timeout
        true    -> ok  %% TODO(ionel): implement balancing
    end,
    balance_workers(Rest).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
assign_workers([], [], _WPerScheduler, _ExtraWorkers, Assignments) ->
    Assignments;
assign_workers(Workers, [Scheduler | RestS], WPerScheduler, ExtraWorkers,
               Assignments) ->
    if ExtraWorkers > 0 ->
            {SchedulerW, RestW} = lists:split(WPerScheduler + 1, Workers),
            assign_workers(RestW, RestS, WPerScheduler, ExtraWorkers - 1,
                           [{Scheduler, SchedulerW} | Assignments]);
       true             ->
            {SchedulerW, RestW} = lists:split(WPerScheduler, Workers),
            assign_workers(RestW, RestS, WPerScheduler, ExtraWorkers,
                           [{Scheduler, SchedulerW} | Assignments])
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
remove_worker_scheduler(SName, Worker) ->
    lists:delete(Worker, get_scheduler_workers(SName)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_new_workers(Workers) ->
    lists:filter(fun(W) ->
                         case ets:lookup(worker_scheduler, W) of
                             [] -> true;
                             _  -> false
                         end end, Workers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_scheduler_workers(SName) ->
    {_, WLs} = lists:unzip(ets:lookup(scheduler_assig, SName)),
    lists:concat(WLs).

%% TODO(ionel): Implement a cleverer reassignemt. At the moment it just adds
%% all the workers to the first scheduler that has a load > 0.8. If there is no
%% such scheduler, it just assigns them to the first one.
%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
reassign_workers(Ws) ->
    Loads = ets:tab2list(scheduler_load),
    WsLeft = reassign_workers(Ws, Loads),
    case WsLeft of
        [] -> ok;
        _  -> case Loads of
                  [] -> error_logger:error_msg("No schedulers available");
                  [{SName, _}] -> append_to_scheduler(SName, WsLeft)
              end
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
reassign_workers(Ws, [{SName, {Load, Time}} | _Rest]) when Load > 0.8 ->
    append_to_scheduler(SName, Ws),
    [];
reassign_workers(Ws, []) ->
    Ws;
reassign_workers(Ws, [_Load | Rest]) ->
    reassign_workers(Ws, Rest).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
append_to_scheduler(SName, Ws) ->
    ets:insert(scheduler_assig,
               {SName, lists:append(get_scheduler_workers(SName), Ws)}).
