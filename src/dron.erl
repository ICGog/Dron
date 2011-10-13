-module(dron).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

%-------------------------------------------------------------------------------

start() ->
    application:start(dron).

stop() ->
    application:stop(dron).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

start(_Type, _Args) ->
    error_logger:logfile({open, "log/dron.log"}),
    dron_mnesia:start([node()], [{n_ram_copies, 1}]),
    dron_pool:start(),
    dron_scheduler:start(),
    ok.

stop(_State) ->
    dron_mnesia:stop(),
    ok.
