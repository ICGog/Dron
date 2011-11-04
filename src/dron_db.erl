-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([store_job/1, get_job/1, store_job_instance/1, get_job_instance/1,
         get_job_instance/2, archive_job/1, store_worker/1, delete_worker/1,
         set_worker_status/2, set_failed_worker/1, get_workers/1,
         get_job_instances_on_worker/1]).

%-------------------------------------------------------------------------------

store_job(Job) ->
    Trans = fun() ->
                    case mnesia:wread({jobs, Job#job.name}) of
                        [OldJob] -> mnesia:write({jobs_archive, OldJob});
                        _        -> ok
                    end,
                    mnesia:write(jobs, Job, write)
            end,            
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.
            
get_job(Name) ->
    Trans = fun() ->
                    mnesia:read(jobs, Name, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, [Job]}   -> {ok, Job};
        {atomic, []}      -> {error, no_job};
        {aborted, Reason} -> {error, Reason}
    end.

store_job_instance(JobInstance) ->
    Trans = fun() ->
                    mnesia:write(job_instances, JobInstance, write)
            end,            
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

get_job_instance(Jid) ->
    Trans = fun() ->
                    mnesia:read(job_instances, Jid, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, [JobInstance]} -> {ok, JobInstance};
        {atomic, []}            -> {error, no_job_instance};
        {aborted, Reason}       -> {error, Reason}
    end.

get_job_instance(JName, RTime) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([JI || JI <- mnesia:table(job_instances),
                                          JI#job_instance.name == JName,
                                          JI#job_instance.run_time == RTime]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, [JobInstance]} -> {ok, JobInstance};
        {atomic, []}            -> {error, no_job_instance};
        {aborted, Reason}       -> {error, Reason}
    end.
        
archive_job(Name) ->
    Trans = fun() ->
                    case mnesia:wread({jobs, Name}) of
                        [Job] -> ok = mnesia:delete({jobs, Name}),
                                 mnesia:write(jobs_archive, Job, write);
                        _     -> no_such_job
                    end
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

store_worker(Worker) ->
    Trans = fun() ->
                    mnesia:write(workers, Worker, write)
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

delete_worker(WName) ->
    Trans = fun() ->
                    mnesia:delete({workers, WName})
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

% enabled = true | false.
set_worker_status(WName, Enabled) ->
    Trans = fun() ->
                    case mnesia:wread({workers, WName}) of
                        [Worker] -> mnesia:write(workers, Worker#worker{
                                                            enabled = Enabled},
                                                write);
                        _        -> no_such_worker
                    end
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

set_failed_worker(WName) ->
    Trans = fun() ->
                    case mnesia:wread({workers, WName}) of
                        [Worker] -> mnesia:write(workers, Worker#worker{
                                                            enabled = false,
                                                            used_slots = 0},
                                                 write);
                        _        -> no_such_worker
                    end
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

get_workers(Enabled) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([W || W <- mnesia:table(workers),
                                   W#worker.enabled == Enabled]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> Return;
        {aborted, Reason} -> {error, Reason}
    end.

get_job_instances_on_worker(WName) ->
    Trans = fun() ->
                    qlc:eval(qlc:q([JI || JI <- mnesia:table(job_instances),
                                          JI#job_instance.worker == WName]))
            end,
    case mnesia:transaction(Trans) of
        {atomic, Return}  -> Return;
        {aborted, Reason} -> {error, Reason}
    end.
