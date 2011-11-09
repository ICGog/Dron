-module(dron_test).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([register_long_jobs/1, register_long_jobs/2, register_short_jobs/1,
        register_short_jobs/2]).

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
