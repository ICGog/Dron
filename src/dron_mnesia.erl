-module(dron_mnesia).

-export([start/0, stop/0, create_job_table/0, put_job/1, get_next_job/0]).

-include("dron.hrl").

%-------------------------------------------------------------------------------
% API
%-------------------------------------------------------------------------------

start() ->
    ok = mnesia:delete_schema([node()|nodes()]),
    ok = mnesia:create_schema([node()]),
    ok = mnesia:start().

stop() ->
    stopped = mnesia:stop(),
    ok = mnesia:delete_schema().

create_job_table() ->
    {atomic, ok} = 
        mnesia:create_table(?JOB_REGISTRY_TABLE,
                            [{ram_copies, [node()]},
                             {record_name, dron_job},
                             {attributes, record_info(fields, dron_job)},
                             {type, ordered_set}]),
    ?JOB_REGISTRY_TABLE.

put_job(DronJob) ->
    mnesia:sync_transaction(fun () ->
                                    mnesia:write(?JOB_REGISTRY_TABLE,
                                                 DronJob,
                                                 sticky_write) end).

get_next_job() ->
    GetJob = fun() -> case mnesia:first(?JOB_REGISTRY_TABLE) of
                          '$end_of_table' -> {no_jobs};
                          Res             -> {ok, Res}
                      end
             end,
    mnesia:sync_transaction(GetJob).
