-module(dron_config).
-author("Ionel Corneliu Gog").

-export([scheduler_nodes/0, db_nodes/0, max_slots/0, exchanges/0,
         dron_exchange/0, consumers/0, log_dir/0, master_nodes/0,
         expand_node_names/1]).

%-------------------------------------------------------------------------------

scheduler_nodes() ->
    Nodes = expand_node_names("DRON_SCHEDULERS"),
    case Nodes of
        [] -> [node()];
        _  -> Nodes
    end.

db_nodes() ->
    Nodes = expand_node_names("DRON_DB"),
    case Nodes of
        [] -> [node()];
        _  -> Nodes
    end.    

max_slots() ->
    500.

exchanges() ->
    [{<<"dron_events">>, <<"fanout">>},
    {<<"hadoop_events">>, <<"fanout">>},
    {<<"spark_events">>, <<"fanout">>}].

consumers() ->
    [{dron_event_consumer, <<"dron_events">>, <<"">>}].

dron_exchange() ->
    <<"dron_events">>.

log_dir() ->
    "/var/log/dron/".

master_nodes() ->
    Nodes = expand_node_names("DRON_MASTERS"),
    case Nodes of
        [] -> [node()];
        _  -> Nodes
    end.

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
expand_node_names(EnvVar) ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    case os:getenv(EnvVar) of
        false ->
            [];
        WorkersEnv ->
            lists:map(fun(Worker) ->
                              list_to_atom(
                                case lists:member($@, Worker) of
                                    true  -> Worker;
                                    false -> Worker ++ "@" ++ Host
                                end)
                      end, string:tokens(WorkersEnv, " \n\t"))
    end.
