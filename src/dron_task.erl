-module(dron_task).

-export([start_link/, run/2]).

-include("dron.hrl").

%-------------------------------------------------------------------------------

start_link(Job) ->
    spawn_link(fun () -> process_flag(trap_exit, true),
                         spawn_loop([Job], 0, none)
               end).

spawn_loop(_, 10, Reason) ->
    exit({max_failures, Reason});
spawn_loop(Args, NFail, _Reason) ->
    Pid = dron_pool:pspawn_link(dron_task, run, [Args + self()]),
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

run(Cmd, Tasklet) ->
    Output = os:cmd(Cmd),
    case Status = os:cmd("$?") of
        "0" -> Tasklet ! {ok, Output};
        _   -> Tasklet ! {error, Output}
    end,
    ok.
