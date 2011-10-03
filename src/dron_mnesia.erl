-module(dron_mnesia).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/0, stop/0]).
-export([create_job_table/0, delete_table/1]).

%-------------------------------------------------------------------------------

start() ->
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start().

stop() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema([node()]).

create_job_table() ->
    {atomic, ok} =
        mnesia:create_table(
          jobs,
          [{record_name, job},
           {attributes, record_info(fields, job)},
           {type, set},
           {disc_copies, [node()]}]).

delete_table(Name) ->
    mnesia:delete_table(Name).
