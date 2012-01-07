-module(mnesia_test).
-author("Ionel Corneliu Gog").
-include_lib("stdlib/include/qlc.hrl").

-export([start/2, start_node/1, stop/0, stop_node/1]).

-export([store_job/1, get_job/1, store_job_instance/1, get_job_instance/1]).

-export([register_short_jobs/1, register_short_jobs/2,
         count_jobs_and_instances/0]).

-export([create_job_instance/1]).

-record(job, {name, cmd_line, start_time, frequency, timeout, max_retries,
             dependencies, deps_timeout}).

-record(job_instance, {jid, name, cmd_line, state, timeout, run_time, num_retry,
                       deps_timeout, dependencies, worker}).

%===============================================================================

start(Nodes, Mode) ->
    ok = mnesia:create_schema(Nodes),
    lists:map(fun(Node) ->
                ok = rpc:call(Node, mnesia, start, []) end, Nodes),
    create_jobs_table(Nodes, Mode),
    create_job_instances_table(Nodes, Mode),
    ok.

start_node(Node) ->
    ok = mnesia:create_schema([Node]),
    ok = rpc:call(Node, mnesia, start, []),
    {ok, _RetValue} = mnesia:change_config(extra_db_nodes, [Node]),
    {atomic, ok} = mnesia:change_table_copy_type(schema, Node, disc_copies),
    {atomic, ok} = mnesia:change_table_frag(jobs, {add_node, Node}),
    {atomic, ok} = mnesia:change_table_frag(job_instances, {add_node, Node}),
    ok.

stop() ->
    {db_nodes, Nodes} = mnesia:system_info(db_nodes),
    lists:map(fun stop_node/1, Nodes),
    ok.

stop_node(Node) ->
    ok = mnesia:delete_schema([Node]),
    ok = rpc:call(Node, mnesia, stop, []).

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
        {atomic, _JobInstances} -> {error, multiple_job_instances};
        {aborted, Reason}       -> {error, Reason}
    end.

register_short_jobs(NumJobs) ->
    register_short_jobs(1, NumJobs).

register_short_jobs(NumStartJob, NumEndJob) ->
    StartTime = calendar:local_time(),
    lists:map(fun(Num) ->
                      Job = #job{name = "short" ++ integer_to_list(Num),
                                 cmd_line = "sleep 0",
                                 start_time = StartTime,
                                 frequency = 10,
                                 timeout = 10,
                                 max_retries = 1,
                                 dependencies = [],
                                 deps_timeout = 10},
                      timer:apply_interval(10000, mnesia_test,
                                           create_job_instance,
                                           [Job]),
                      store_job(Job)
              end, lists:seq(NumStartJob, NumEndJob)).

create_job_instance(#job{name = Name, cmd_line = Cmd, timeout = Timeout,
                         dependencies = Deps}) ->
    RunTime = calendar:local_time(),
    JId = {Name, RunTime},
    spawn(fun() -> store_job_instance(
                     #job_instance{jid = JId, name = Name,
                                   cmd_line = Cmd,
                                   state = waiting, timeout = Timeout,
                                   run_time = RunTime,
                                   num_retry = 0,
                                   dependencies = Deps,
                                   worker = undefined})
          end).

count_jobs_and_instances() ->
    Trans = fun() ->
                    {lists:sum(qlc:eval(
                                 qlc:q([1 || _J <- mnesia:table(jobs)]))),
                     lists:sum(
                       qlc:eval(
                         qlc:q([1 || _JI <- mnesia:table(job_instances)])
                        ))}
            end,
    mnesia:transaction(Trans).

%===============================================================================
% Internal
%===============================================================================

create_jobs_table(Nodes, Mode) ->
    {atomic, ok} =
        mnesia:create_table(
          jobs,
          [{record_name, job},
           {attributes, record_info(fields, job)},
           {type, set},
           {frag_properties, [{node_pool, Nodes},
                              {n_fragments, length(Nodes)}] ++ Mode}]).

create_job_instances_table(Nodes, Mode) ->
    {atomic, ok} =
        mnesia:create_table(
          job_instances,
          [{record_name, job_instance},
           {attributes, record_info(fields, job_instance)},
           {type, set},
           {frag_properties, [{node_pool, Nodes},
                              {n_fragments, length(Nodes)}] ++ Mode}]).
