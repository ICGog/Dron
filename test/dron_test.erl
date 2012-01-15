-module(dron_test).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([register_long_jobs/1, register_long_jobs/2, register_short_jobs/1,
         register_short_jobs/2, register_short_jobs/3, populate_db/4,
         count_jobs_and_instances/0, generate_node_names/1]).

%-------------------------------------------------------------------------------

register_long_jobs(NumJobs) ->
    register_long_jobs(1, NumJobs).

register_long_jobs(NumStartJob, NumEndJob) ->
    StartTime = calendar:local_time(),
    lists:map(fun(Num) ->
                      dron_api:register_job(#job{name = "long" ++
                                                     integer_to_list(Num),
                                                 cmd_line = "sleep 600",
                                                 start_time = StartTime,
                                                 frequency = 601,
                                                 timeout = 700,
                                                 max_retries = 3,
                                                 dependencies = [],
                                                 deps_timeout = 10})
              end, lists:seq(NumStartJob, NumEndJob)).    

register_short_jobs(NumJobs) ->
    register_short_jobs(1, NumJobs).

register_short_jobs(NumStartJob, NumEndJob) ->
    StartTime = calendar:local_time(),
    lists:map(fun(Num) ->
                      dron_api:register_job(#job{name = "short" ++
                                                     integer_to_list(Num),
                                                 cmd_line = "sleep 0",
                                                 start_time = StartTime,
                                                 frequency = 10,
                                                 timeout = 10,
                                                 max_retries = 1,
                                                 dependencies = [],
                                                 deps_timeout = 10})
              end, lists:seq(NumStartJob, NumEndJob)).

register_short_jobs(NumStartJob, NumEndJob, _Increment)
  when NumStartJob > NumEndJob ->
    ok;
register_short_jobs(NumStartJob, NumEndJob, Increment)
  when NumStartJob =< NumEndJob ->
    StartTime = calendar:local_time(),
    NumEnd = if NumStartJob + Increment > NumEndJob ->
                     NumEndJob;
                true                                ->
                     NumStartJob + Increment
             end,
    lists:map(fun(Num) ->
                      dron_api:register_job(
                        #job{name = "short" ++ integer_to_list(Num),
                             cmd_line = "sleep 0",
                             start_time = StartTime,
                             frequency = 10,
                             timeout = 10,
                             max_retries = 1,
                             dependencies = [],
                             deps_timeout = 10})
              end, lists:seq(NumStartJob, NumEnd)),
    timer:sleep(1000),
    register_short_jobs(NumStartJob + Increment + 1, NumEndJob, Increment).

populate_db(NumJobs, Name, State, Worker) ->
    lists:map(fun(Num) ->
                      JName = Name ++ integer_to_list(Num),
                      dron_db:store_job(
                        #job{name = JName, cmd_line = "ls", start_time = 0,
                             frequency = 1, timeout = 1,
                             max_retries = 3, dependencies = [],
                             deps_timeout = 10}),
                      lists:map(fun(_JINum) ->
                                        dron_db:store_job_instance(
                                          #job_instance{
                                             jid = {node(), now()},
                                             name = JName, cmd_line = "ls",
                                             state = State, timeout = 1,
                                             run_time = 1, num_retry = 1,
                                             deps_timeout = 10,
                                             dependencies = [],
                                             worker = Worker})
                                end, lists:seq(1, 10))
              end, lists:seq(1, NumJobs)).

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

generate_node_names(Names) ->
    Machines = lists:map(
                 fun({Name, {From, To}}) ->
                         M = lists:map(fun(Num) ->
                                               if
                                                   Num < 10 ->
                                                       "icg08@" ++ Name ++
                                                           "0" ++
                                                           integer_to_list(Num);
                                                   true ->
                                                       "icg08@" ++ Name ++
                                                           integer_to_list(Num)
                                               end
                                       end, lists:seq(From, To)),
                         lists:foldl(fun(Node, Acc) -> Node ++ " " ++ Acc end,
                                     "", M)
                 end, Names),
    Res = lists:foldl(fun(Node, Acc) -> Node ++ " " ++ Acc end, "", Machines),
    file:write_file("machines", io_lib:fwrite("~p", [Res])).
