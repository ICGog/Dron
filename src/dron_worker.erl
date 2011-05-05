-module(dron_worker).

-export([run_cmd/2]).

run_cmd(Cmd, Master) ->
    Output = os:cmd(Cmd),
    case Status = os:cmd("$?") of
        "0" -> Master ! {ok, Output};
        _   -> Master ! {error, Output}
    end,
    ok.
