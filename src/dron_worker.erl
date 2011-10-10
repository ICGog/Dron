-module(dron_worker).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([start/1, run/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%--------------------------------------------------------------------------------

start(WName) ->
    gen_server:start_link({global, WName}, ?MODULE, [], []).

run(WName, Cmd) ->
    gen_server:cast({global, WName}, {run, Cmd}).

%--------------------------------------------------------------------------------
% Internal
%--------------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast({run, Cmd}, State) ->
    Output = os:cmd(Cmd),
    file:write_file("output", io_lib:fwrite("~p", [Output])),
    {noreply, State};
handle_cast(_Request, _State) ->
    not_implemented.

handle_info(_Info, _State) ->
    not_implemented.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
