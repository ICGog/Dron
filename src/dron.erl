-module(dron).

-behaviour(application).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

start() ->
    ok = error_logger:logfile({open, "log/dron.log"}),
    application:start(dron).

stop() ->
    dron_pool:kill_workers(),
    dron_pool:dettach_workers(),
    application:stop(dron).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start(_Type, _Args) ->
    {ok, Sup} = dron_sup:start_link(),
    dron_mnesia:start(),
    dron_mnesia:create_job_table(),
    dron_pool:auto_attach_workers(),
    {ok, Sup}.

stop(_State) ->
    ok.
