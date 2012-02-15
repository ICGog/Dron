-module(dron_ns_hdfs).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([create_file/3, delete_file/3, create_dir/3, delete_dir/3]).

%===============================================================================

create_file(From, Node, NS = #name_server{name = Path}) ->
    Reply = ok,
    gen_server:reply(From, Reply).

delete_file(From, Node, Path) ->
    Reply = ok,
    gen_server:reply(From, Reply).

create_dir(From, Node, NS = #name_server{name = Path}) ->
    Reply = ok,
    gen_server:reply(From, Reply).

delete_dir(From, Node, Path) ->
    Reply = ok,
    gen_server:reply(From, Reply).
