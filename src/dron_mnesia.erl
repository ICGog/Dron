-module(dron_mnesia).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/0, stop/0]).
-export([create_jobs_table/0, delete_table/1]).

%-------------------------------------------------------------------------------

start() ->
    ok = mnesia:create_schema([node()]),
    create_jobs_table(),
    create_job_instances_table(),
    ok = mnesia:start().

stop() ->
    stopped = mnesia:stop(),
    delete_table(jobs),
    delete_table(job_instances),
    ok = mnesia:delete_schema([node()]).

create_jobs_table() ->
    {atomic, ok} =
        mnesia:create_table(
          jobs,
          [{record_name, job},
           {attributes, record_info(fields, job)},
           {type, set},
           {disc_copies, [node()]}]).

create_job_instances_table() ->
    {atomic, ok} =
        mnesia:create_table(
          job_instances,
          [{record_name, job_instance},
           {attributes, record_info(fields, job_instance)},
           {type, set},
           {disc_copies, [node()]}]).

delete_table(Name) ->
    mnesia:delete_table(Name).
