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
    application:stop(dron).

%-------------------------------------------------------------------------------
% Internal API
%-------------------------------------------------------------------------------

start(_Type, _Args) ->
    {ok}.

stop(_State) ->
    ok.
