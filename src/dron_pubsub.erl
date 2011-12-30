-module(dron_pubsub).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_consumer/3, publish_message/3, setup_exchange/2,
         stop_exchange/1]).

-record(state, {connection, channel}).

%===============================================================================

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% @spec start_consumer(Module, Exchange, RoutingKey) -> ok
%% @end
%%------------------------------------------------------------------------------
start_consumer(Module, Exchange, RoutingKey) ->
    gen_server:call(?NAME, {start_consumer, Module, Exchange, RoutingKey}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec publish_message(Exchange, RoutingKey, Message) -> ok
%% @end
%%------------------------------------------------------------------------------
publish_message(Exchange, RoutingKey, Payload) ->
    gen_server:call(?NAME, {publish_message, Exchange, RoutingKey, Payload}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec setup_exchange(ExchangeName, Type) -> ok | error
%% @end
%%------------------------------------------------------------------------------
setup_exchange(Name, Type) ->
    gen_server:call(?NAME, {setup_exchange, Name, Type}).

%%------------------------------------------------------------------------------
%% @doc
%% @spec stop_exchange(ExchangeName) -> ok | error
%% @end
%%------------------------------------------------------------------------------
stop_exchange(Exchange) ->
    gen_server:call(?NAME, {stop_exchange, Exchange}).

%===============================================================================
% Internal
%===============================================================================

%% IMPORTANT: At the moment it only uses a connection and a channel for all
%% the messages. Check how this affects the perfomance.
%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    % It does not matter whether the exchanges were already declared or not.
    lists:map(fun({Exchange, Type}) ->
                      setup_exchange_intern(Channel, Exchange, Type) end,
              dron_config:exchanges()),
    lists:map(fun({Module, Exchange, RoutingKey}) ->
                      start_consumer_intern(Channel, Module, Exchange,
                                            RoutingKey)
              end, dron_config:consumers()),
    {ok, #state{connection = Connection, channel = Channel}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({start_consumer, Module, Exchange, RoutingKey}, _From,
            State = #state{channel = Channel}) ->
    start_consumer_intern(Channel, Module, Exchange, RoutingKey),
    {reply, ok, State};
handle_call({setup_exchange, Name, Type}, _From,
            State = #state{channel = Channel}) ->
    {reply, setup_exchange_intern(Channel, Name, Type), State};
handle_call({stop_exchange, Exchange}, _From,
            State = #state{channel = Channel}) ->
    {reply, stop_exchange_intern(Channel, Exchange), State};
handle_call({publish_message, Exchange, RoutingKey, Payload}, _From,
            State = #state{channel = Channel}) ->
    ok = amqp_channel:cast(Channel,
                           #'basic.publish'{exchange = Exchange,
                                            routing_key = RoutingKey},
                           #amqp_msg{payload = Payload}),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    error_logger:error_msg("Got unexpected call ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(Request, State) ->
    error_logger:error_msg("Got unexpected cast ~p", [Request]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    error_logger:error_msg("Got unexpected message ~p", [Info]),
    {stop, not_supported, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, #state{channel = Channel, connection = Connection}) ->
    % The exchange is not stopped to that frameworks can still publish messages.
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

start_consumer_intern(Channel, Module, Exchange, RoutingKey) ->
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    Binding = #'queue.bind'{queue = Queue,
                            exchange = Exchange,
                            routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    proc_lib:start_link(Module, init, [self(), Channel, Queue]).

setup_exchange_intern(Channel, Name, Type) ->
    Exchange = #'exchange.declare'{exchange = Name, type = Type},
    case amqp_channel:call(Channel, Exchange) of
        #'exchange.declare_ok'{} -> ok;
        _                        -> error
    end.

stop_exchange_intern(Channel, Exchange) ->
    case amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange}) of
        #'exchange.delete_ok'{} -> ok;
        _                       -> error
    end.
