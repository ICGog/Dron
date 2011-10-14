-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([get_new_id/0, store_object/1, delete_object/1, get_job/1,
         archive_job/1]).

%-------------------------------------------------------------------------------

get_new_id() ->
    mnesia:dirty_update_counter(ids, id, 1).

store_object(Object) ->
    Trans = fun() ->
                    mnesia:write(Object)
            end,
    mnesia:transaction(Trans).

delete_object(Object) ->
    Trans = fun() ->
                    mnesia:delete_object(Object)
            end,
    mnesia:transaction(Trans).

get_job(Name) ->
    Trans = fun() ->
                    mnesia:match_object(jobs, {job, Name, '_', '_', '_', '_'},
                                        read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, Job} -> {ok, Job};
        {aborted, Reason} -> {error, Reason}
    end.

get_job_instance(Jid) ->
    Trans = fun() ->
                    mnesia:match_object(job_instances, {job_instance, Jid, '_',
                                                        '_', '_', '_'}, read)
            end,
    case mnesia:transaction(Trans) of
        {atomic, JobInstance} -> {ok, JobInstance};
        {aborted, Reason} -> {error, Reason}
    end.

archive_job(Name) ->
    ok.
