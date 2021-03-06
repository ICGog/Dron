-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([store_job/1, get_job/1, get_job_unsync/1, store_job_instance/1,
         get_job_instance/1, get_job_instance_unsync/1, get_job_instance/2,
         set_job_instance_state/2, archive_job/1, store_worker/1,
         delete_worker/1, get_worker/1, get_workers/1, get_last_run_time/1,
         update_workers_scheduler/2, get_workers_of_scheduler/1,
         get_job_instances_on_worker/1, get_dependants/1, store_dependant/2,
         set_resource_state/2, adjust_slot/2]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @spec store_job(Job) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_job(Job) ->
  Trans = fun() -> 
                case mnesia:wread({jobs, Job#job.name}) of
                  [OldJob] -> mnesia:write({jobs_archive, OldJob});
                  []       -> ok
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
    {atomic, [Job]}   -> {ok, Job};
    {atomic, []}      -> {error, no_job};
    {atomic, _Jobs}   -> {error, multiple_jobs};
    {aborted, Reason} -> {error, Reason}
  end.

%%------------------------------------------------------------------------------
%% @doc
%% Get the job. The method performs a dirty read.
%%
%% @spec get_job_unsync(Name) -> {ok, Job} | {error, no_job}
%%     | {error, multiple_jobs} |
%%        {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_job_unsync(Name) ->
  case (catch mnesia:dirty_read(jobs, Name)) of
    [Job]  -> {ok, Job};
    []     -> {error, no_job};
    Reason -> {error, Reason}
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
get_job_instance(JId) ->
  Trans = fun() ->
                  mnesia:read(job_instances, JId, read)
          end,
  case mnesia:transaction(Trans) of
    {atomic, [JobInstance]} -> {ok, JobInstance};
    {atomic, []}            -> {error, no_job_instance};
    {atomic, _JobInstances} -> {error, multiple_job_instances};
    {aborted, Reason}       -> {error, Reason}
  end.

get_job_instance_unsync(JId) ->
  case (catch mnesia:dirty_read(job_instances, JId)) of
    [JobInstance] -> {ok, JobInstance};
    []            -> {error, no_job_instance};
    Reason        -> {error, Reason}
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

get_last_run_time(JName) ->
  Trans = fun() ->
              qlc:eval(qlc:q([JI || JI <- mnesia:table(job_instances),
                                    JI#job_instance.name == JName]))
          end,
  {atomic, JIs} = mnesia:transaction(Trans),
  get_last_run(JIs, {{1970, 1, 1}, {1, 0, 0}}).

get_last_run([], RT) ->
  RT;
get_last_run([#job_instance{run_time = JRT} | JIs], RT) ->
  if
    JRT > RT ->
      get_last_run(JIs, JRT);
    true ->
      get_last_run(JIs, RT)
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
%% @spec get_worker(WorkerName) -> ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_worker(WName) ->
  Trans = fun() ->
                  mnesia:read({workers, WName})
          end,
  case mnesia:transaction(Trans) of
    {atomic, [Worker]} -> {ok, Worker};
    {atomic, []}       -> {error, no_worker};
    {atomic, _Workers} -> {error, multiple_workers};
    {aborted, Reason}  -> {error, Reason}
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
%% @spec adjust_slot(WName, Add) ->
%%     ok | {error, Reason} | {error, unknown_worker}
%% @end
%%------------------------------------------------------------------------------
adjust_slot(WName, Add) ->
  Trans = fun() ->
                  case mnesia:wread({workers, WName}) of
                    [W = #worker{used_slots = UsedSlots}] ->
                      mnesia:write(workers, 
                                   W#worker{used_slots = UsedSlots + Add},
                                   write);
                    [] -> unknown_worker
                  end
          end,
  case mnesia:transaction(Trans) of
    {atomic, ok}      -> ok;
    {atomic, Return}  -> {error, Return};
    {aborted, Reason} -> {error, Reason}
  end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec get_workers_of_scheduler(SchedulerName) ->
%%    {ok, [Workers]} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
get_workers_of_scheduler(SName) ->
  Trans = fun() ->
                  qlc:eval(qlc:q([W || W <- mnesia:table(workers),
                                       W#worker.scheduler == SName,
                                       W#worker.enabled == true]))
          end,
  case mnesia:transaction(Trans) of
    {atomic, Return}  -> {ok, Return};
    {aborted, Reason} -> {error, Reason}
  end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec update_workers_scheduler(OldScheduler, NewScheduler) ->
%%    ok | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
update_workers_scheduler(OldSched, NewSched) ->
  Trans = fun() ->
                  Ws = qlc:eval(qlc:q([W || W <- mnesia:table(workers),
                                            W#worker.scheduler == OldSched])),
                  lists:map(
                    fun(W) ->
                            mnesia:write(workers,
                                         W#worker{scheduler = NewSched},
                                         write)
                    end, Ws)
          end,
  case mnesia:transaction(Trans) of
    {atomic, _Return}  -> ok;
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
%% @spec store_dependant(Dependencies, JobInstanceId) ->
%%        [UnsatisfiedDependencies] | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
store_dependant(Dependencies, JId) ->
  Trans = fun() -> write_deps(Dependencies, JId, []) end,
  case mnesia:transaction(Trans) of
    {atomic, UnsatisfiedDeps} -> UnsatisfiedDeps;
    {aborted, Reason}         -> {error, Reason}
  end.

%%------------------------------------------------------------------------------
%% @doc
%% @spec set_resource_state(ResourceId, State) -> {ok, JIds} | {error, Reason}
%% @end
%%------------------------------------------------------------------------------
set_resource_state(RId, State) ->
  % TODO(ionel): Check if can improve this. It may have some race conditions
  % because first I read all the res_deps with a given RId, then I remove
  % them and finally I add them back. I am forced to do this because the
  % table is of type bag. I may solve the problem with an additional set
  % table.
  Trans = fun() ->
                  ResDeps = mnesia:wread({resource_deps, RId}),
                  mnesia:delete({resource_deps, RId}),
                  lists:map(
                    fun(ResDep) ->
                            mnesia:write(resource_deps,
                                         ResDep#resource_deps{state = State},
                                         write),
                            ResDep#resource_deps.dep
                    end, ResDeps)
          end,
  case mnesia:transaction(Trans) of
    {atomic, JIds}    -> {ok, JIds};
    {aborted, Reason} -> {error, Reason}
  end.

%%------------------------------------------------------------------------------
%% @private
%% @doc
%% Writes dependencies to the database and returns the list of unsatisfied ones.
%%
%% @spec write_deps(Dependencies, JobInstanceId, UnsatisfiedDependencies) ->
%%        [ResourceIds]
%% @end
%%------------------------------------------------------------------------------
write_deps([], _JId, UnsatisfiedDeps) ->
  UnsatisfiedDeps;
write_deps([Dep|Dependencies], JId, UnsatisfiedDeps) ->
  State = case mnesia:wread({resource_deps, Dep}) of
              [#resource_deps{state = RState}|_] -> RState;
              []                                 -> unsatisfied
          end,
  ok = mnesia:write(resource_deps,
                    #resource_deps{rid = Dep, state = State, dep = JId},
                    write),
  case State of
    satisfied -> write_deps(Dependencies, JId, UnsatisfiedDeps);
    _         -> write_deps(Dependencies, JId, [Dep|UnsatisfiedDeps])
  end.
