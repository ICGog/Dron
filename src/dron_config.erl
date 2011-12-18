-module(dron_config).
-author("Ionel Corneliu Gog").

-export([max_slots/0, dron_exchanges/0, dron_exchange/0, dron_consumers/0,
        dron_log_dir/0]).

%-------------------------------------------------------------------------------

max_slots() ->
    20.

dron_exchanges() ->
    [{<<"dron_events">>, <<"fanout">>},
    {<<"hadoop_events">>, <<"fanout">>},
    {<<"spark_events">>, <<"fanout">>}].

dron_consumers() ->
    [{dron_event_consumer, <<"dron_events">>, <<"">>}].

dron_exchange() ->
    <<"dron_events">>.

dron_log_dir() ->
    "/var/log/dron/".
