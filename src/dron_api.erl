-module(dron_api).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([register_job/1, unregister_job/1, kill_job_instance/2]).

%===============================================================================

%% @TODO: These calls may have synchronization issues.
%%------------------------------------------------------------------------------
%% @doc
%% @spec register_job(Job) -> ok
%% @end
%%------------------------------------------------------------------------------
register_job(Job) ->
    ok = dron_db:store_job(Job),
    dron_scheduler:schedule(Job).

%%------------------------------------------------------------------------------
%% @doc
%% @spec unregister_job(Job) -> ok
%% @end
%%------------------------------------------------------------------------------
unregister_job(Job) ->
    dron_scheduler:unschedule(Job),
    ok = dron_db:archive_job(Job#job.name).

%%------------------------------------------------------------------------------
%% @doc
%% @spec kill_job_instance(JobName, RunTime) -> ok
%% @end
%%------------------------------------------------------------------------------
kill_job_instance(JName, RTime) ->
    {ok, #job_instance{jid = JId, worker = WName}} =
        dron_db:get_job_instance(JName, RTime),
    killed = dron_worker:kill_job_instance(WName, JId, false).
