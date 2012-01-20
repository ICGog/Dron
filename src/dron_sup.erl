-module(dron_sup).
-author("Ionel Corneliu Gog").
-behaviour(supervisor).

-export([start/2]).
-export([init/1]).

%-------------------------------------------------------------------------------

start(Nodes, WorkerAssig) ->
    supervisor:start_link(dron_sup, [Nodes, WorkerAssig]).

init([Nodes, WorkerAssig]) ->
    {ok, {{one_for_one, 1, 60},
         [{coordinator, {dron_coordinator, start_link,
                         [dron_config:master_nodes(), Nodes, WorkerAssig]},
           permanent, 60, worker, [dron_coordinator]},
          {pubsub, {dron_pubsub, start_link, []}, permanent, 60, worker,
           [dron_pubsub]}]}}.
