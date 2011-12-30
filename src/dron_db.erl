-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([store_job/1, get_job/1, store_job_instance/1, get_job_instance/1,
         get_job_instance/2, set_job_instance_state/2, archive_job/1,
         store_worker/1, delete_worker/1, get_workers/1,
         get_job_instances_on_worker/1, get_dependants/1,
         store_dependant/2]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_job(Job) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_job(Job) ->
    Trans = fun() ->
                    case mnesia:wread({jobs, Job#job.name}) of
                        [OldJob]  -> mnesia:write({jobs_archive, OldJob});
                        []        -> ok
                    end,
                    mnesia:write(jobs, Job, write)
            end,            
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_job(Name) -> {ok, Job} | {error, no_job} | {error, multiple_jobs} |
%%        {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_job(Name) ->
    Trans = fun() ->
                    mnesia:read(jobs, Name, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, [Job]}         -> {ok, Job};
        {atomic, []}            -> {error, no_job};
        {atomic, _Jobs}         -> {error, multiple_jobs};
        {aborted, Reason}       -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_job_instance(JobInstance) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_job_instance(JobInstance) ->
    Trans = fun() ->
                    mnesia:write(job_instances, JobInstance, write)
            end,            
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_job_instance(JobInstanceId) -> {ok, JobInstance} |
%%        {error, no_job_instance} | {error, multiple_job_instances} |
%%        {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_job_instance(Jid) ->
    Trans = fun() ->
                    mnesia:read(job_instances, Jid, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, [JobInstance]} -> {ok, JobInstance};
        {atomic, []}            -> {error, no_job_instance};
        {atomic, _JobInstances} -> {error, multiple_job_instances};
        {aborted, Reason}       -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_job_instance(JobName, RunTime) -> {ok, JobInstance} |
%%        {error, no_job_instance} | {error, multiple_job_instances} |
%%        {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_job_instance(JName, RTime) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([JI || JI <- mnesia:table(job_instances),
                                          JI#job_instance.name == JName,
                                          JI#job_instance.run_time == RTime]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, [JobInstance]} -> {ok, JobInstance};
        {atomic, []}            -> {error, no_job_instance};
        {atomic, _JobInstances} -> {error, multiple_job_instances};
        {aborted, Reason}       -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec set_job_instance_state(JobInstanceId, State) -> ok |
%%        {error, no_such_job_instance} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
set_job_instance_state(JId, State) ->
    Trans = fun() ->
                    case mnesia:wread({job_instances, JId}) of
                        [JI] -> mnesia:write(job_instances,
                                             JI#job_instance{state = State},
                                             write);
                        []   -> no_such_job_instance
                    end
            end,                                      
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {atomic, Return}  -> {error, Return};
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec archive_job(JobName) -> ok | {error, no_such_job} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
archive_job(Name) ->
    Trans = fun() ->
                    case mnesia:wread({jobs, Name}) of
                        [Job] -> ok = mnesia:delete({jobs, Name}),
                                 mnesia:write(jobs_archive, Job, write);
                        []    -> no_such_job
                    end
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {atomic, Return}  -> {error, Return};
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_worker(Worker) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_worker(Worker) ->
    Trans = fun() ->
                    mnesia:write(workers, Worker, write)
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec delete_worker(WorkerName) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
delete_worker(WName) ->
    Trans = fun() ->
                    mnesia:delete({workers, WName})
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_workers(Enabled) -> {ok, [Workers]} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_workers(Enabled) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([W || W <- mnesia:table(workers),
                                   W#worker.enabled == Enabled]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> {ok, Return};
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_job_instances_on_worker(WorkerName) -> {ok, [JobInstances]} |
%%      {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_job_instances_on_worker(WName) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([JI || JI <- mnesia:table(job_instances),
                                          JI#job_instance.worker == WName]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> {ok, Return};
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_dependants(ResourceId) -> {ok, [JobInstancesId]} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_dependants(RId) ->
    Trans = fun() ->
                    mnesia:read(resource_deps, RId, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> {ok, Return};
        {aborted, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_dependant(Dependants, ResourceId) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_dependant(Dependants, RId) ->
    Trans = fun() ->
                    lists:map(fun(Dep) ->
                                      ok = mnesia:write(
                                             resource_deps,
                                             #resource_deps{rid = Dep,
                                                            dep = RId},
                                             write)
                              end, Dependants),
                    ok
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.
