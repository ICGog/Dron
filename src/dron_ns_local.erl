-module(dron_ns_local).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([create_file/3, delete_file/3, create_dir/3, delete_dir/3]).

%===============================================================================

create_file(From, Node, NS = #name_server{name = Path}) ->
    Reply =
        case ets:lookup(name_server, Path) of
            [] ->
                case rpc:call(Node, dron_worker, create_file, [Node, Path]) of
                    ok -> ets:insert(name_server, {Path, NS}),
                          ok;
                    {error, _} -> {error, path}
                end;
            _  ->
                {error, already_exists}
        end,
    gen_server:reply(From, Reply).

delete_file(From, Node, Path) ->
    Reply =
        case ets:lookup(name_server, Path) of
            [] ->
                {error, not_exists};
            [_NS] ->
                case rpc:call(Node, dron_worker, delete_file, [Node, Path]) of
                    ok -> ets:delete(name_server, Path),
                          ok;
                    {error, _} -> {error, path}
                end
        end,
    gen_server:reply(From, Reply).

create_dir(From, Node, NS = #name_server{name = Path}) ->
    Reply =
        case ets:lookup(name_server, Path) of
            [] ->
                case rpc:call(Node, dron_worker, create_dir, [Node, Path]) of
                    ok -> ets:insert(name_server, {Path, NS}),
                          ok;
                    {error, _} -> {error, path}
                end;
            _  ->
                {error, already_exists}
        end,
    gen_server:reply(From, Reply).

delete_dir(From, Node, Path) ->
    Reply =
        case ets:lookup(name_server, Path) of
            [] ->
                {error, not_exists};
            [_NS] ->
                case rpc:call(Node, dron_worker, delete_dir, [Node, Path]) of
                    ok -> ets:delete(name_server, Path),
                          ok;
                    {error, _} -> {error, path}
                end
        end,
    gen_server:reply(From, Reply).
