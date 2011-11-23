-module(dron_api).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([register_job/1, unregister_job/1, kill_job_instance/2]).

%-------------------------------------------------------------------------------

%% @TODO: These calls may have synchronization issues.
register_job(Job) ->
    ok = dron_db:store_job(Job),
    dron_scheduler:schedule(Job).

unregister_job(Job) ->
    dron_scheduler:unschedule(Job),
    ok = dron_db:archive_job(Job#job.name).

kill_job_instance(JName, RTime) ->
    {ok, #job_instance{jid = JId, worker = WName}} =
        dron_db:get_job_instance(JName, RTime),
    killed = dron_worker:kill_job_instance(WName, JId, false).
