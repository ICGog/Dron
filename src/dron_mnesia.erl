-module(dron_mnesia).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([start/3, start_node/1, stop/0, stop_node/1]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @spec start(Nodes, Mode) -> ok
%% @end
%%------------------------------------------------------------------------------
start(Nodes, Mode, MnesiaNodes) ->
  Ret = mnesia:create_schema(Nodes),
  error_logger:info_msg("Create schema returned ~p", [Ret]),
  lists:map(fun(Node) ->
              RetStart = rpc:call(Node, mnesia, start, []),
              error_logger:info_msg("Got ~p while starting mnesia on ~p",
                                    [RetStart, Node]) end, Nodes),
  case Ret of
    ok ->
      create_jobs_table(Nodes, Mode),
      create_jobs_archive_table(Nodes, Mode),
      create_job_instances_table(Nodes, Mode),
      create_job_instance_deps_table(Nodes, Mode),
      create_workers_table(Nodes, Mode);
    _ ->
      ok
  end,
  ok.

%%------------------------------------------------------------------------------
%% @doc
%% @spec start_node(Node) -> ok
%% @end
%%------------------------------------------------------------------------------
start_node(Node) ->
  ok = mnesia:create_schema([Node]),
  ok = rpc:call(Node, mnesia, start, []),
  {ok, _RetValue} = mnesia:change_config(extra_db_nodes, [Node]),
  {atomic, ok} = mnesia:change_table_copy_type(schema, Node, disc_copies),
  {atomic, ok} = mnesia:change_table_frag(jobs, {add_node, Node}),
  {atomic, ok} = mnesia:change_table_frag(jobs_archive, {add_node, Node}),
  {atomic, ok} = mnesia:change_table_frag(job_instances, {add_node, Node}),
  {atomic, ok} = mnesia:change_table_frag(resource_deps, {add_node, Node}),
  {atomic, ok} = mnesia:change_table_frag(workers, {add_node, Node}),
  ok.

%%------------------------------------------------------------------------------
%% @doc
%% @spec stop() -> ok
%% @end
%%------------------------------------------------------------------------------
stop() ->
  Nodes = lists:delete(node(), mnesia:system_info(db_nodes)),
  error_logger:info_msg("Stopping Mnesia on nodes ~p", [Nodes]),
  lists:foreach(fun(Node) ->
                      rpc:call(Node, mnesia, stop, []) end,
                lists:reverse(Nodes)),
  mnesia:delete_schema(Nodes),
  ok.

%%------------------------------------------------------------------------------
%% @doc
%% @spec stop_node(Node) -> ok
%% @end
%%------------------------------------------------------------------------------
stop_node(Node) ->
  ok = rpc:call(Node, mnesia, stop, []),
  ok = mnesia:delete_schema([Node]).

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

create_job_instance_deps_table(Nodes, Mode) ->
  {atomic, ok} =
      mnesia:create_table(
        resource_deps,
        [{record_name, resource_deps},
         {attributes, record_info(fields, resource_deps)},
         {type, bag},
         {frag_properties, [{node_pool, Nodes},
                            {n_fragments, length(Nodes)}] ++ Mode}]).

create_workers_table(Nodes, Mode) ->
  {atomic, ok} =
      mnesia:create_table(
        workers,
        [{record_name, worker},
         {attributes, record_info(fields, worker)},
         {type, set},
         {frag_properties, [{node_pool, Nodes},
                            {n_fragments, length(Nodes)}] ++ Mode}]).
