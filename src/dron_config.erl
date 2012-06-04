-module(dron_config).
-author("Ionel Corneliu Gog").

-export([scheduler_nodes/0, db_nodes/0, max_slots/0, exchanges/0,
         dron_exchange/0, consumers/0, log_dir/0, master_nodes/0,
         worker_nodes/0, expand_node_names/1, scheduler_heartbeat_interval/0,
         scheduler_heartbeat_timeout/0, worker_low_load/0, worker_medium_load/0,
         worker_high_load/0, min_backoff/0, max_backoff/0]).

%-------------------------------------------------------------------------------

scheduler_nodes() ->
  Nodes = expand_node_names("DRON_SCHEDULERS"),
  case Nodes of
    [] -> [node()];
    _  -> Nodes
  end.

db_nodes() ->
  Nodes = expand_node_names("DRON_DB"),
  case Nodes of
    [] -> [node()];
    _  -> Nodes
  end.    

max_slots() ->
  1000.

exchanges() ->
  [{<<"dron_events">>, <<"fanout">>},
  {<<"hadoop_events">>, <<"fanout">>},
  {<<"spark_events">>, <<"fanout">>}].

consumers() ->
  [{dron_event_consumer, <<"dron_events">>, <<"">>}].

dron_exchange() ->
  <<"dron_events">>.

log_dir() ->
  "/var/log/dron/".

master_nodes() ->
  Nodes = expand_node_names("DRON_MASTERS"),
  case Nodes of
    [] -> [node()];
    _  -> Nodes
  end.

worker_nodes() ->
  Nodes = expand_node_names("DRON_WORKERS"),
  case Nodes of
    [] -> [node()];
    _  -> Nodes
  end.

scheduler_heartbeat_interval() ->
  10000.

scheduler_heartbeat_timeout() ->
  {0, {0, 0, 30}}.

%% {WorkerOfferings, WorkersRequests}. The first value represents an upper bound
%% up to which the worker pool is offering workers. The second value represents
%% an upper bound up to which the scheduler is not asking for extra workers.
worker_low_load() ->
  {0.3, 0.9}.

worker_medium_load() ->
  {0.2, 0.8}.

worker_high_load() ->
  {0.1, 0.7}.

min_backoff() ->
  100.

max_backoff() ->
  409600.

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
expand_node_names(EnvVar) ->
  [_, Host] = string:tokens(atom_to_list(node()), "@"),
  case os:getenv(EnvVar) of
    false ->
      [];
    WorkersEnv ->
      lists:map(fun(Worker) ->
                      list_to_atom(
                        case lists:member($@, Worker) of
                          true  -> Worker;
                          false -> Worker ++ "@" ++ Host
                        end)
                end, string:tokens(WorkersEnv, " \n\t"))
  end.
