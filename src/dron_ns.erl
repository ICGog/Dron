-module(dron_ns).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, append/1, create_file/2, create_file/3, delete_file/1,
         exists/1, create_dir/2, create_dir/3, delete_dir/1, open_stream/1,
         rename/2]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start the name server.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%------------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% 
%%
%% @spec append(Path) -> ok | {error, not_file} | {error, not_exists}
%% @end
%%------------------------------------------------------------------------------
%% TODO(ionel): Implement.
append(Path) ->
    todo.

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec create_file(FileSystem, Path) -> ok | {error, path}
%%     | {error, already_exists}
%% @end
%%------------------------------------------------------------------------------
create_file(FileSystem, Path) ->
    gen_server:call({global, ?MODULE},
                    {create_file, #name_server{name = Path,
                                               location = FileSystem}}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec create_file(local, Node, Path) -> ok | {error, path}
%%     | {error, already_exists}
%% @end
%%------------------------------------------------------------------------------
create_file(FileSystem, Node, Path) when FileSystem == local ->
    gen_server:call({global, ?MODULE},
                    {create_file, #name_server{name = Path,
                                               location = {local, Node}}}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec delete_file(Path) -> ok | {error, not_exists}
%% @end
%%------------------------------------------------------------------------------
delete_file(Path) ->
    gen_server:call({global, ?MODULE}, {delete_file, Path}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec exists(Path) -> true | false
%% @end
%%------------------------------------------------------------------------------
exists(Path) ->
    gen_server:call({global, ?MODULE}, {exists, Path}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec create_dir(FileSystem, Path) ->
%%     ok | {error, path} | {error, already_exists}
%% @end
%%------------------------------------------------------------------------------
create_dir(FileSystem, Path) ->
    gen_server:call({global, ?MODULE},
                    {create_dir, #name_server{name = Path,
                                              location = FileSystem}}).

create_dir(FileSystem, Node, Path) when FileSystem == local ->
    gen_server:call({global, ?MODULE},
                    {create_dir, #name_server{name = Path,
                                              location = {local, Node}}}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec delete_dir(Path) -> ok | {error, not_exists}
%% @end
%%------------------------------------------------------------------------------
delete_dir(Path) ->
    gen_server:call({global, ?MODULE}, {delete_dir, Path}).

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec open_stream(Path) -> ok
%% @end
%%------------------------------------------------------------------------------
open_stream(Path) ->
%% TODO(ionel): Implement.
    todo.

%%------------------------------------------------------------------------------
%% @doc
%%
%% @spec rename(SourcePath, DestinationPath) -> ok | {error, source}
%%     | {error, destination}
%% @end
%%------------------------------------------------------------------------------
rename(SourcePath, DestinationPath) ->
    todo.

%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    ets:new(name_server, [ordered_set, named_table, public]),
    {ok, []}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({create_file, NS = #name_server{location = Location}},
            From, State) ->
    {Module, Args} = file_system_info(Location),
    erlang:spawn_link(Module, create_file, [From] ++ Args ++ [NS]),
    {noreply, State};
handle_call({delete_file, Path}, From, State) ->
    case get_info_from_path(Path) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {Module, Args}  ->
            erlang:spawn_link(Module, delete_file, [From] ++ Args ++ [Path]),
            {noreply, State}
    end;
handle_call({exists, Path}, _From, State) ->
    case get_info_from_path(Path) of
        {error, Reason} ->
            {reply, false, State};
        {Module, Args}  ->
            {reply, true, State}
    end;
handle_call({create_dir, NS = #name_server{location = Location}},
            From, State) ->
    {Module, Args} = file_system_info(Location),
    erlang:spawn_link(Module, create_dir, [From] ++ Args ++ [NS]),
    {noreply, State};
handle_call({delete_dir, Path}, From, State) ->
    case get_info_from_path(Path) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {Module, Args}  ->
            erlang:spawn_link(Module, delete_dir, [From] ++ Args ++ [Path]),
            {noreply, State}
    end;
handle_call(_Request, _From, State) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(_Request, State) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info(Message, State) ->
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

file_system_info({local, Node}) ->
    {dron_ns_local, [Node]};
file_system_info(hdfs) ->
    {dron_ns_hdfs, []}.

get_info_from_path(Path) ->
    case ets:lookup(name_server, Path) of
        [] -> {error, not_exists};
        [{_Path, #name_server{location = Location}}] ->
            file_system_info(Location)
    end.
