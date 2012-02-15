-module(dron_stream).
-author("Ionel Corneliu Gog").

-export([start_client/1, send/2, close/1, start_server/1, stream_cmd/2]).

%===============================================================================

start_client(Port) ->
    {ok, Socket} = gen_tcp:connect("localhost", Port, [binary, {packet, 0}]),
    Socket.

send(Socket, Buffer) ->
    ok = gen_tcp:send(Socket, Buffer).

close(Socket) ->
    ok = gen_tcp:close(Socket).

start_server(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {packet, 0}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(LSock),
    Bytes = receive_bytes(Socket, []),
    ok = gen_tcp:close(Socket),
    Bytes.

receive_bytes(Socket, Bs) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Bytes} -> receive_bytes(Socket, [Bs, Bytes]);
        {error, closed} -> list_to_binary(Bs)
    end.

stream_cmd(Cmd, Port) ->
    Socket = start_client(Port),
    unix_cmd(Cmd, Socket).
    
%===============================================================================
% Internal
%===============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
unix_cmd(Cmd, Socket) ->
    Tag = make_ref(),
    {Pid, MRef} = erlang:spawn_monitor(
                    fun() ->
                            process_flag(trap_exit, true),
                            Port = start_port(),
                            erlang:port_command(Port, mk_cmd(Cmd)),
                            exit({Tag, unix_get_data(Port, Socket)})
                    end),
    receive
        {'DOWN', MRef, _, Pid, {Tag, ok}} ->
            ok;
        {'DOWN', MRef, _, Pid, Reason} ->
            exit(Reason)
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
unix_get_data(Port, Socket) ->
    receive
	{Port,{data, Bytes}} ->
	    case eot(Bytes) of
		{done, Last} ->
                    send(Socket, Last),
                    close(Socket),
                    ok;
		more ->
                    send(Socket, Bytes),
		    unix_get_data(Port, Socket)
	    end;
	{'EXIT', Port, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
mk_cmd(Cmd) when is_atom(Cmd) ->
    mk_cmd(atom_to_list(Cmd));
mk_cmd(Cmd) ->
    io_lib:format("(~s\n) </dev/null; echo  \"\^D\"\n", [Cmd]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
eot(Bs) ->
    eot(Bs, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
eot([4| _Bs], As) ->
    {done, lists:reverse(As)};
eot([B| Bs], As) ->
    eot(Bs, [B| As]);
eot([], _As) ->
    more.




-define(SHELL, "/bin/sh -s unix:cmd 2>&1").
-define(PORT_CREATOR_NAME, os_cmd_port_creator).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start_port() ->
    Ref = make_ref(),
    Request = {Ref,self()},    
    {Pid, Mon} = case whereis(?PORT_CREATOR_NAME) of
		     undefined ->
			 spawn_monitor(fun() ->
					       start_port_srv(Request)
				       end);
		     P ->
			 P ! Request,
			 M = erlang:monitor(process, P),
			 {P, M}
		 end,
    receive
	{Ref, Port} when is_port(Port) ->
	    erlang:demonitor(Mon, [flush]),
	    Port;
	{Ref, Error} ->
	    erlang:demonitor(Mon, [flush]),
	    exit(Error);
	{'DOWN', Mon, process, Pid, _Reason} ->
	    start_port()
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start_port_srv(Request) ->
    %% We don't want a group leader of some random application. Use
    %% kernel_sup's group leader.
    {group_leader, GL} = process_info(whereis(kernel_sup),
				      group_leader),
    true = group_leader(GL, self()),
    process_flag(trap_exit, true),
    StayAlive = try register(?PORT_CREATOR_NAME, self())
		catch
		    error:_ -> false
		end,
    start_port_srv_handle(Request),
    case StayAlive of
	true -> start_port_srv_loop();
	false -> exiting
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start_port_srv_handle({Ref,Client}) ->
    Reply = try open_port({spawn, ?SHELL},[stream]) of
		Port when is_port(Port) ->
		    (catch port_connect(Port, Client)),
		    unlink(Port),
		    Port
	    catch
		error:Reason ->
		    {Reason,erlang:get_stacktrace()}	    
	    end,
    Client ! {Ref,Reply}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start_port_srv_loop() ->
    receive
	{Ref, Client} = Request when is_reference(Ref),
				     is_pid(Client) ->
	    start_port_srv_handle(Request);
	_Junk ->
	    ignore
    end,
    start_port_srv_loop().
