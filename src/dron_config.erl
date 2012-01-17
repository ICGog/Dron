-module(dron_config).
-author("Ionel Corneliu Gog").

-export([max_slots/0, exchanges/0, dron_exchange/0, consumers/0, log_dir/0,
         master_nodes/0]).

%-------------------------------------------------------------------------------

max_slots() ->
    10000.

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
    [node()].
