-module(dron_monitor).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([new_scheduler_leader/2, start_new_workers/3, store_new_workers/2,
        start_new_scheduler/3, add_workers/2, add_scheduler/3,
        remove_scheduler/2, remove_workers/1, remove_num_workers/2,
        remove_workers/2, remove_workers_from_memory/2, assign_workers/1,
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
%% @spec store_new_workers(SchedulerName, Workers) -> ok
%% @end
%%------------------------------------------------------------------------------
store_new_workers(SName, Ws) ->
    lists:map(fun(W) -> ets:insert(worker_scheduler, {W, SName}) end, Ws),
    NewWs = lists:append(get_scheduler_workers(SName), Ws),
    ets:insert(scheduler_assig, {SName, NewWs}),
    ok.

%%------------------------------------------------------------------------------
%% @doc
%% Starts a new scheduler together with its workers. The workers that are used
%% by other schedulers are not added.
%%
%% @spec start_new_scheduler(Schedulers, SchedulerName, Workers) ->
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
                               start, [[SName], node(), worker_low_load]) of
                     {ok, _Pid} ->
                         ets:insert(scheduler_heartbeat,
                                    {SName, {alive, calendar:local_time()}}),
                         timer:sleep(200),
                         start_new_workers(SName, get_new_workers(Workers));
                     Reason     ->
                         {error, Reason}
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
    Res = rpc:call(SName, dron_pool, offer_workers, [NewWs]),
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
                     _  -> ets:insert(scheduler_heartbeat,
                             {SName, {alive, calendar:local_time()}}),
                           store_new_workers(SName, NewWs),
                           NewWs
                 end
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Removes a scheduler. Its workers do not get reassigned.
%%
%% @spec remove_scheduler(Schedulers, SchedulerName) ->
%%    RemWs | {error, unknown_scheduler}
%% @end
%%------------------------------------------------------------------------------
remove_scheduler(Schedulers, SName) ->
    case lists:member(SName, Schedulers) of
        false -> {error, unknown_scheduler};
        true  -> ets:delete(scheduler_heartbeat, SName),
                 Ws = get_scheduler_workers(SName),
                 ets:delete(scheduler_assig, SName),
                 lists:map(fun(W) ->
                                   ets:insert(worker_scheduler,
                                              {W, unallocated})
                           end, Ws),
                 Ws
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
%% Removes a given number of workers from a scheduler.
%%
%% @spec remove_num_workers(SchedulerName, NumWorkers) -> RemovedWorkers
%% @end
%%------------------------------------------------------------------------------
remove_num_workers(SName, NumW) ->
    SWs = get_scheduler_workers(SName),
    LenSWs = length(SWs),
    {RemovedWs, RemainedWs} = if NumW > LenSWs -> {SWs, []};
                                 true          -> lists:split(NumW, SWs)
                              end,
    rpc:call(SName, dron_pool, take_workers, [RemovedWs]),
    ets:insert(scheduler_assig, {SName, RemainedWs}),
    lists:map(fun(W) ->
                      ets:insert(worker_scheduler, {W, unallocated})
              end, RemovedWs),
    RemovedWs.

%%------------------------------------------------------------------------------
%% @doc
%% Removes a given list of workers from a scheduler.
%%
%% @spec remove_workers(SchedulerName, Workers) -> ok
%% @end
%%------------------------------------------------------------------------------
remove_workers(SName, Ws) ->
    SWs = get_scheduler_workers(SName),
    rpc:call(SName, dron_pool, take_workers, [Ws]),
    ets:insert(scheduler_assig, {SName, lists:subtract(SWs, Ws)}),
    lists:map(fun(W) ->
                      ets:insert(worker_scheduler, {W, unallocated})
              end, Ws),
    ok.

%%------------------------------------------------------------------------------
%% @doc
%% Removes from the memory a given list of workers from a scheduler.
%%
%% @spec remove_workers_from_memory(SchedulerName, Workers) -> ok
%% @end
%%------------------------------------------------------------------------------
remove_workers_from_memory(SName, Ws) ->
    SWs = get_scheduler_workers(SName),
    ets:insert(scheduler_assig, {SName, lists:subtract(SWs, Ws)}),
    lists:map(fun(W) ->
                      ets:delete(worker_scheduler, W)
              end, Ws),
    ok.

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
    ets:delete(scheduler_heartbeat, OldSched),
    lists:map(fun(Worker) ->
                      ets:insert(worker_scheduler, {Worker, NewSched})
              end, Ws),
    ets:insert(scheduler_assig, {NewSched, Ws}),
    ets:insert(scheduler_heartbeat, {NewSched, {alive, calendar:local_time()}}),
    ok.

%%------------------------------------------------------------------------------
%% @doc
%% Assigns workers stored in environment variable to the given list of
%% schedulers.
%%
%% @spec assign_workers(Schedulers) -> [{Scheduler, Workers}]
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

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec balance_workers(Heartbeats) -> ok
%% @end
%%------------------------------------------------------------------------------
balance_workers(Heartbeats) ->
    {Failed, Offers, Requests} = split_heartbeats(Heartbeats, [], [], []),
    ExtraWs = handle_failed_schedulers(Failed, []),
    SOffers = lists:sort(fun compare_heartbeats/2, Offers),
    SRequests = lists:sort(fun compare_heartbeats/2, Requests),
    NOfferW = sum_heartbeats(SOffers, 0),
    {_RemWs, RRequests} = allocate_workers(ExtraWs, SRequests),
    NReqW = sum_heartbeats(RRequests, 0),
    if NReqW > 0 ->
            {Ws, ROffers} = get_offers(SOffers, NOfferW, NReqW, [], 0, []),
            LWs = length(Ws),
            OWs = if LWs < NReqW ->
                          {NWs, _} = get_first_offers(ROffers, NReqW - LWs,
                                                      [], Ws),
                          NWs;
                     true ->
                          Ws
                  end,
            allocate_workers(OWs, RRequests);
       true -> ok
    end.
        
%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
allocate_workers(Ws, Reqs) ->
    NReqW = sum_heartbeats(Reqs, 0),
    {RWs, Requests, NSatis} = satisfy_requests(Ws, length(Ws), Reqs, NReqW,
                                               0, []),
    LRWs = length(RWs),
    if
        NSatis < NReqW andalso LRWs > 0 ->
            satisfy_first_requests(RWs, Requests, []);
        true ->
            {RWs, Requests}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
compare_heartbeats({_, {{_, NW1}, _}}, {_, {{_, NW2}, _}}) ->
    if NW1 >= NW2 -> true;
       true       -> false
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
sum_heartbeats([], Sum) ->
    Sum;
sum_heartbeats([{_, {{_, WNum}, _}} | Rest], Sum) ->
    sum_heartbeats(Rest, Sum + WNum).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_failed_schedulers([], Ws) ->
    Ws;
handle_failed_schedulers([{SName, _} | Rest], Ws) ->
    SWs = dron_coordinator:remove_scheduler(SName),
    error_logger:error_msg("Scheduler ~p went down. Reassigning workers ~p",
                           [SName, SWs]),
    handle_failed_schedulers(Rest, lists:append(SWs, Ws)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
split_heartbeats([], Failed, Offers, Requests) ->
    {Failed, Offers, Requests};
split_heartbeats([{_SName, {Req, Time}} = Heartbeat | Rest], Failed, Offers,
                 Requests) ->
    TimeDiff = calendar:time_difference(Time, calendar:local_time()),
    Expired = TimeDiff > dron_config:scheduler_heartbeat_timeout(),
    if
        Expired ->
            split_heartbeats(Rest, [Heartbeat | Failed], Offers, Requests);
        true    ->
            case Req of 
                {offer, _} -> split_heartbeats(Rest, Failed,
                                               [Heartbeat | Offers], Requests);
                {request, _} -> split_heartbeats(Rest, Failed, Offers,
                                                 [Heartbeat | Requests]);
                alive -> split_heartbeats(Rest, Failed, Offers, Requests)
            end
    end.

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
    rpc:cast(SName, dron_pool, take_workers, [Worker]),
    lists:delete(Worker, get_scheduler_workers(SName)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_new_workers(Workers) ->
    lists:filter(fun(W) ->
                         case ets:lookup(worker_scheduler, W) of
                             [] -> true;
                             [{_, unallocated}] -> true;
                             _  -> false
                         end end, Workers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_scheduler_workers(SName) ->
    {_, WLs} = lists:unzip(ets:lookup(scheduler_assig, SName)),
    lists:concat(WLs).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
satisfy_requests(Ws, _NWs, [], _NReqW, NSatis, Heartbeats) ->
    {Ws, Heartbeats, NSatis};
satisfy_requests([], _NWs, Requests, _NReqW, NSatis, Heartbeats) ->
    {[], lists:append(lists:reverse(Heartbeats), Requests), NSatis};
satisfy_requests(Ws, NWs,
                 [{SName, {{request, NumW}, Time}} = Heartbeat | Rest],
                 NReqW, NSatis, Heartbeats) ->
    OfferNumW = if
                    NReqW > NWs -> min(NumW, NWs - NSatis);
                    true        -> min(NumW * NReqW div NWs, NWs - NSatis)
                end,
    case OfferNumW of
        0 -> {Ws, lists:append(lists:reverse(Heartbeats),
                               [Heartbeat | Rest]), NSatis};
        _ -> {AssignWs, RemWs} = lists:split(OfferNumW, Ws),
             AddedWs = dron_coordinator:add_workers(SName, AssignWs),
             LAddedWs = length(AddedWs),
             satisfy_requests(
               RemWs, NWs, Rest, NReqW, NSatis + LAddedWs,
               [{SName, {{request, NumW - LAddedWs}, Time}} | Heartbeats])
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
satisfy_first_requests([], Heartbeats, NHeartbeats) ->
    {[], lists:append(Heartbeats, lists:reverse(NHeartbeats))};
satisfy_first_requests(Ws, [], NHeartbeats) ->
    {Ws, lists:reverse(NHeartbeats)};
satisfy_first_requests([W | Ws],
                       [{SName, {{request, NumW}, Time}} = Heartbeat | Rest],
                       Heartbeats) ->
    case NumW of
        0 -> satisfy_first_requests([W | Ws], Rest, [Heartbeat | Heartbeats]);
        _ -> dron_coordinator:add_workers(SName, [W]),
             satisfy_first_requests(
               Ws, Rest, [{SName, {{request, NumW - 1}, Time}} | Heartbeats])
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_offers([], _NOffW, _NReqW, Heartbeats, _NumWs, Ws) ->
    {Ws, lists:reverse(Heartbeats)};
get_offers(Offers, _NOffW, NReqW, Heartbeats, NReqW, Ws) ->
    {Ws, lists:append(lists:reverse(Heartbeats), Offers)};
get_offers([{SName, {{offer, NumW}, Time}} = Heartbeat | Rest], NOffW, NReqW,
           Heartbeats, NWs, Ws) ->
    TakeNumW = if
                   NReqW > NOffW -> NumW;
                   true          -> NumW * NReqW div NOffW
               end,
    case TakeNumW of
        0 -> {Ws, lists:append(lists:reverse(Heartbeats),
                               [Heartbeat | Rest])};
        _ -> RemWs = dron_coordinator:remove_workers(SName, TakeNumW),
             LRemWs = length(RemWs),
             get_offers(Rest, NOffW, NReqW,
                        [{SName, {{offer, NumW - LRemWs}, Time}} | Heartbeats],
                        NWs + LRemWs, lists:append(RemWs, Ws))
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_first_offers([], _NReqW, Heartbeats, Ws) ->
    {Ws, Heartbeats};
get_first_offers(Heartbeats, 0, NHeartbeats, Ws) ->
    {Ws, lists:append(lists:reverse(NHeartbeats), Heartbeats)};
get_first_offers([{SName, {{offer, NumW}, Time}} = Heartbeat | Rest], NReqW,
                 Heartbeats, Ws) ->
    case NumW of
        0 -> get_first_offers(Rest, NReqW, [Heartbeat | Heartbeats], Ws);
        _ -> RemWs = dron_coordinator:remove_workers(SName, 1),
             LRemWs = length(RemWs),
             get_first_offers(
               Rest, NReqW - LRemWs,
               [{SName, {{offer, NumW - LRemWs}, Time}} | Heartbeats],
               [RemWs | Ws])
    end.
