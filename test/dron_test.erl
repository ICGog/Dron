-module(dron_test).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([register_long_jobs/1, register_long_jobs/2, register_short_jobs/1,
        register_short_jobs/2, populate_db/4, count_jobs_and_instances/0,
        generate_node_names/1]).

%-------------------------------------------------------------------------------

register_long_jobs(NumJobs) ->
    register_long_jobs(NumJobs, 0).

register_long_jobs(NumJobs, StartTime) ->
    lists:map(fun(Num) ->
                      dron_api:register_job(#job{name = "long" ++
                                                     integer_to_list(Num),
                                                 cmd_line = "sleep 6000",
                                                 start_time = StartTime,
                                                 frequency = 6010000,
                                                 timeout = 7000000,
                                                 max_retries = 3})
              end, lists:seq(1, NumJobs)).

register_short_jobs(NumJobs) ->
    register_short_jobs(NumJobs, 0).

register_short_jobs(NumJobs, StartTime) ->
    lists:map(fun(Num) ->
                      dron_api:register_job(#job{name = "short" ++
                                                     integer_to_list(Num),
                                                 cmd_line = "sleep 60",
                                                 start_time = StartTime,
                                                 frequency = 61000,
                                                 timeout = 70000,
                                                 max_retries = 1})
              end, lists:seq(1, NumJobs)).

populate_db(NumJobs, Name, State, Worker) ->
    lists:map(fun(Num) ->
                      JName = Name ++ integer_to_list(Num),
                      dron_db:store_job(
                        #job{name = JName, cmd_line = "ls", start_time = 0,
                             frequency = 1000, timeout = 1000,
                             max_retries = 3}),
                      lists:map(fun(_JINum) ->
                                        dron_db:store_job_instance(
                                          #job_instance{
                                             jid = {node(), now()},
                                             name = JName, cmd_line = "ls",
                                             state = State, timeout = 1000,
                                             run_time = 1000, num_retry = 1,
                                             worker = Worker})
                                end, lists:seq(1, 10))
              end, lists:seq(1, NumJobs)).

count_jobs_and_instances() ->
    Trans = fun() ->
                    {lists:sum(qlc:eval(qlc:q([1 || J <- mnesia:table(jobs)]))),
                     lists:sum(qlc:eval(
                                 qlc:q([1 || JI <- mnesia:table(job_instances)])
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
