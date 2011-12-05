-module(dron_config).
-author("Ionel Corneliu Gog").

-export([max_slots/0, dron_consumer/0, dron_exchanges/0, job_instance_key/0]).

%-------------------------------------------------------------------------------

max_slots() ->
    20.

dron_consumer() ->
    dron_consumer.

dron_exchanges() ->
    [{<<"dron_job_instances">>, <<"direct">>}].

job_instance_key() ->
    <<"dron_job_instance">>.
