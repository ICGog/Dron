-module(dron_mnesia).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/2, start_node/1, stop/0, stop_node/1]).

%-------------------------------------------------------------------------------

start(Nodes, Mode) ->
    ok = mnesia:create_schema(Nodes),
    lists:map(fun(Node) ->
                ok = rpc:call(Node, mnesia, start, []) end, Nodes),
    create_jobs_table(Nodes, Mode),
    create_jobs_archive_table(Nodes, Mode),
    create_job_instances_table(Nodes, Mode),
    create_ids_table(Nodes, Mode),
    ok.

start_node(Node) ->
    ok = mnesia:create_schema([Node]),
    ok = rpc:call(Node, mnesia, start, []),
    {ok, _RetValue} = mnesia:change_config(extra_db_nodes, [Node]),
    {atomic, ok} = mnesia:change_table_copy_type(schema, Node, disc_copies),
    ok.

stop() ->
    {db_nodes, Nodes} = mnesia:system_info(db_nodes),
    lists:map(fun stop_node/1, Nodes),
    ok.

stop_node(Node) ->
    ok = mnesia:delete_schema([Node]),
    ok = rpc:call(Node, mnesia, stop, []).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

create_jobs_table(Nodes, Mode) ->
    {atomic, ok} =
        mnesia:create_table(
          jobs,
          [{record_name, job},
           {attributes, record_info(fields, job)},
           {type, set},
           {frag_properties, [{node_pool, Nodes},
                              {n_fragments, length(Nodes)}] ++ Mode}]).

create_jobs_archive_table(Nodes, Mode) ->
    {atomic, ok} =
        mnesia:create_table(
          jobs_archive,
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

create_ids_table(Nodes, Mode) ->
    {atomic, ok} =
        mnesia:create_table(
          ids,
          [{record_name, id},
           {attributes, record_info(fields, id)},
           {type, set},
           {frag_properties, [{node_pool, Nodes},
                              {n_fragments, length(Nodes)}] ++ Mode}]).
