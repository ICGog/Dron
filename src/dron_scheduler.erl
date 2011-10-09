-module(dron_scheduler).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start/0, schedule/1, unschedule/1]).

-record(timers, {timers = dict:new()}).

%-------------------------------------------------------------------------------

start() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

schedule(Job) ->
    gen_server:cast(?NAME, {schedule, Job}).

unschedule(Job) ->
    gen_server:cast(?NAME, {unschedule, Job}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast({schedule, Job = #job{name = Name, start_time = STime,
                                  frequency = Freq}},
           #timers{timers = Timers}) ->
    {ok, TRef} = timer:apply_interval(Freq, ?MODULE, run_instance, [Job]),
    {noreply, Timers#timers{timers = dict:store(Name, TRef, Timers)}};
    
handle_cast({unschedule, #job{name = Name}},
           #timers{timers = Timers}) ->
    {ok, TRef} = dict:find(Name, Timers),
    {ok, cancel} = timer:cancel(TRef),
    {noreply, Timers#timers{timers = dict:erase(Name, Timers)}};

handle_cast(_Request, _State) ->
    not_implemented.

handle_info(_Request, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

run_instance(#job{name = Name, cmd_line = Cmd, timeout = Timeout}) ->
    dron_mnesia:write(#job_instance{name = Name, cmd_line = Cmd,
                                    timeout = Timeout, run_time = time()}),
    Worker = dron_pool:get_worker(),
    dron_worker:run_cmd({global, Worker}, Cmd).
