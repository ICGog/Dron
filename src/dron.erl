-module(dron).

-behaviour(application).

-export([start/0, stop/0]).

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

start() ->
    application:start(dron).

stop() ->
    % TODO(ionel): stop all the workers
    application:stop(dron).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start(_Type, _Args) ->
    {ok, Sup} = dron_sup:start_link(),
    ResAttach = dron_master:auto_attach_workers(),
    {ok, Sup}.

stop(_State) ->
    ok.
