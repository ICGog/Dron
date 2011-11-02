-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([store_job/1, get_job/1, store_job_instance/1, get_job_instance/1,
         archive_job/1, store_worker/1, delete_worker/1]).

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
        {aborted, Reason} -> {error, Reason}
    end.

archive_job(Name) ->
    Trans = fun() ->
                    case mnesia:wread({jobs, Name}) of
                        [Job] -> case mnesia:delete({jobs, Name}) of
                                     ok -> mnesia:write({jobs_archive, Job});
                                     _  -> mnesia:abort("Could not delete job")
                                 end;
                        _     -> mnesia:abort("No such job")
                    end
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
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

delete_worker(Name) ->
    Trans = fun() ->
                    mnesia:delete({workers, Name})
            end,
    case mnesia:transaction(Trans) of
        {atomic, ok}      -> ok;
        {aborted, Reason} -> {error, Reason}
    end.