-module(dron_sup).
-author("Ionel Corneliu Gog").
-behaviour(supervisor).

-export([start/0]).
-export([init/1]).

%-------------------------------------------------------------------------------

start() ->
    supervisor:start_link(dron_sup, []).

init([]) ->
    {ok, {{one_for_one, 1, 60},
         [{coordinator, {dron_coordinator, start_link,
                         [dron_config:master_nodes()]},
           permanent, 60, worker, [dron_coordinator]},
          {pubsub, {dron_pubsub, start_link, []}, permanent, 60, worker,
           [dron_pubsub]}]}}.
