-module(dron_api).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([register_job/1, unregister_job/1]).

%-------------------------------------------------------------------------------

register_job(Job) ->
    ok = dron_db:store_job(Job),
    dron_scheduler:schedule(Job).

unregister_job(Job) ->
    dron_scheduler:unschedule(Job),
    ok = dron_db:archive_job(Job).
