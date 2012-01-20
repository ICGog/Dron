-module(dron).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @spec start() -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
start() ->
    application:start(dron).

%%------------------------------------------------------------------------------
%% @doc
%% @spec stop() -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
stop() ->
    application:stop(dron).

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start(_Type, _Args) ->
    dron_mnesia:start(dron_config:db_nodes(), [{n_ram_copies, 2}]),
    error_logger:info_msg("~nMnesia is running!~n", []),
    error_logger:info_msg("~nDron Schedulers are starting...~n", []),
    Schedulers = lists:map(fun(Node) ->
                                   {Node,
                                    rpc:call(Node, dron_scheduler, start,
                                             [[Node]])}
                           end, dron_config:scheduler_nodes()),
    error_logger:info_msg("~nDron schedulers: ~p", [Schedulers]),
    {OkSchedulers, _} = lists:unzip(
                          lists:filter(fun({_Node, Result}) ->
                                               case Result of
                                                   {ok, _Pid} -> true;
                                                   _          -> false
                                               end
                                       end, Schedulers)),
    Assignments = assign_workers(OkSchedulers),
    error_logger:info_msg("Worker assignments: ~p", [Assignments]),
    AssigRes = lists:map(fun({Node, Workers}) ->
                                 {{Node, Workers},
                                  rpc:call(Node, dron_pool, add_workers,
                                           [Workers])}
                         end, Assignments),
    error_logger:info_msg("Pools creation results ~p", [AssigRes]),
    {WorkerAssig, _} = lists:unzip(AssigRes),
    dron_sup:start(dron_config:scheduler_nodes(), WorkerAssig).
    
%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) ->
    dron_pubsub:stop_exchange(dron_config:job_instance_exchange()),
    dron_mnesia:stop(),
    ok.

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
