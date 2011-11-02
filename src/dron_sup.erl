-module(dron_sup).
-author("Ionel Corneliu Gog").
-behaviour(supervisor).

-export([start/0]).
-export([init/1]).

%-------------------------------------------------------------------------------

start() ->
    supervisor:start_link(dron_sup, []).

% Start the pool and the scheduler. In case of any failure they will be
% continuously restarted. They will be terminated by first sending an
% exit(Child, shutdown) and then the supervisor will wait for 60 seconds
% for an exit signal back.
init(_Args) ->
    {ok, {{one_for_one, 0, 1},
         [{pool, {dron_pool, start_link, []}, permanent, 60, worker,
           [dron_pool]},
         {scheduler, {dron_scheduler, start_link, []}, permanent, 60, worker,
          [dron_scheduler]}]}}.
