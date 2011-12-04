-module(dron_thrift).
-author("Ionel Corneliu Gog").
-include("dron_thrift.hrl").

-export([start/1, stop/1, handle_function/2, job_instance_registered/2]).

%-------------------------------------------------------------------------------

job_instance_registered(jobInstanceId, jobInstanceInfo) ->
    error_logger:info_msg("Received message", []).

start(Port) ->
    thrift_socket_server:start([{handler, ?MODULE},
                                {service, dron_thirft},
                                {port, Port},
                                {name, dron_server}]).

stop(Server) ->
    thrift_socket_server:stop(Server).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

handle_function(Function, Args) when is_atom(Function), is_tuple(Args) ->
    case apply(?MODULE, Function, tuple_to_list(Args)) of
        ok    -> ok;
        Reply -> {reply, Reply}
    end.
