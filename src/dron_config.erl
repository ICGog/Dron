-module(dron_config).
-author("Ionel Corneliu Gog").

-export([max_slots/0, dron_exchanges/0, dron_consumers/0]).

%-------------------------------------------------------------------------------

max_slots() ->
    20.

dron_exchanges() ->
    [{<<"dron_job_instances">>, <<"direct">>},
    {<<"hadoop_events">>, <<"fanout">>},
    {<<"hadoop_dron">>, <<"fanout">>}].

dron_consumers() ->
    [{dron_hadoop_consumer, <<"hadoop_events">>, <<"">>},
    {dron_hadoop_consumer, <<"hadoop_dron">>, <<"">>}].
