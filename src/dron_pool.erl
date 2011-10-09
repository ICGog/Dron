-module(dron_pool).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start/0, add_worker/1, remove_worker/1, get_worker/0]).

-record(workers, {workers = []}).

%-------------------------------------------------------------------------------

start() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

add_worker(Worker) ->
    gen_server:call(?NAME, {add, Worker}).

remove_worker(Worker) ->
    gen_server:call(?NAME, {remove, Worker}).

get_worker() ->
    gen_server:call(?NAME, {get_worker}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    {ok, #workers{}}.

handle_call({add, Worker}, _From, State = #workers{workers = Ws}) ->
    case lists:member(Worker, Ws) of
        true  -> {reply, {error, already_added}, State};
        false -> case net_adm:ping(Worker) of
                     pong -> NewWs = [Worker | Ws],
                             NewState = State#workers{workers = NewWs},
                             dron_worker:start({global, Worker}),
                             {reply, ok, NewState};
                     _    -> {reply, {error, no_connection}, State}
                 end
    end;
handle_call({remove, Worker}, _From, State = #workers{workers = Ws}) ->
    case lists:member(Worker, Ws) of
        true  -> NewWs = lists:delete(Worker, Ws),
                 {reply, ok, State#workers{workers = NewWs}};
        false -> {reply, {error, unknown_worker}, State}
    end;
handle_call({get_worker}, _From, State = #workers{workers = Ws}) ->
    case Ws of
        [] -> {reply, {error, no_workers}, State};
        [Worker | Workers] -> {reply, Worker,
                               State#workers{workers = Workers ++ [Worker]}}
    end;
handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast(_Request, _State) ->
    not_implemented.

handle_info(_Request, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
