-module(dron_task).

-export([start_link/1]).

-export([spawn_loop/3, run/2]).

-include("dron.hrl").

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

start_link(Job) ->
    spawn_link(fun () -> process_flag(trap_exit, true),
                         spawn_loop([Job], 0, none)
               end).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

spawn_loop(_, 10, Reason) ->
    exit({max_failures, Reason});
spawn_loop(Args, NFail, _Reason) ->
    Pid = dron_pool:pspawn_link(dron_task, run, Args ++ self()),
    receive
        {'EXIT', Pid, normal} ->
            ok;
        {'EXIT', Pid, Reason} ->
            spawn_loop(Args, NFail + 1, Reason);
        {'EXIT', _, Reason} ->
            exit(Reason);
        {error, Output} -> error_logger:error_msg("Task failed: " + Output),
                           Output;
        {ok, Output} -> error_logger:info_msg("Task ok: " + Output),
                        Output
     end.

run(Cmd, TaskPid) ->
    Output = os:cmd(Cmd),
    case os:cmd("$?") of
        "0" -> TaskPid ! {ok, Output};
        _   -> TaskPid ! {error, Output}
    end,
    ok.
