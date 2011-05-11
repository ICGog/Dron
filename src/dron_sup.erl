-module(dron_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(NAME, {global, ?MODULE}).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link(?NAME, ?MODULE, []).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

init([]) ->
    % If more than 0 processes fail within 10 seconds, all processes are
    % restarted, including the supervisor.
    {ok, {{one_for_all, 0, 10},
          [{master, {dron_master, start_link, []}, permanent, 60,
            worker, [dron_master]},
           {pool, {dron_pool, start_link, []}, permanent, 60,
            worker, [dron_pool]}]}}.
