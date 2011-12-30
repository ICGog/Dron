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
    dron_mnesia:start([node()], [{n_ram_copies, 1}]),
    error_logger:info_msg("~nMnesia is running!~n", []),
    error_logger:info_msg("~nDron Scheduler is starting...~n", []),
    {ok, Sup} = dron_sup:start(),
    error_logger:info_msg("~nDron Scheduler is running!~n", []),
    AutoWorkers = dron_pool:auto_add_workers(),
    error_logger:info_msg("Auto attaching workers: ~w~n", [AutoWorkers]),
    {ok, Sup}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) ->
    dron_pubsub:stop_exchange(dron_config:job_instance_exchange()),
    dron_mnesia:stop(),
    ok.
