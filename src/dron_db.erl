-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([get_new_id/0, get_job/1, archive_job/1, store_worker/1,
         delete_worker/1]).

%-------------------------------------------------------------------------------

% TODO: Check dirty update counter. It can send an exit signal!
get_new_id() ->
    mnesia:dirty_update_counter(ids, id, 1).

get_job(Name) ->
    Trans = fun() ->
                    mnesia:match_object(jobs, {job, Name, '_', '_', '_', '_'},
                                        read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, [Job]}   -> {ok, Job};
        {aborted, Reason} -> {error, Reason}
    end.

get_job_instance(Jid) ->
    Trans = fun() ->
                    mnesia:match_object(job_instances, {job_instance, Jid, '_',
                                                        '_', '_', '_'}, read)
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
