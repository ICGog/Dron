-module(dron_pubsub).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_consumer/2, publish_message/3, setup_exchange/2,
         stop_exchange/1]).

-record(state, {connection, channel, consumers = dict:new()}).

%-------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?NAME, ?MODULE, [], []).

start_consumer(Exchange, RoutingKey) ->
    gen_server:call(?NAME, {start_consumer, Exchange, RoutingKey}).
    
publish_message(Exchange, RoutingKey, Payload) ->
    gen_server:call(?NAME, {publish_message, Exchange, RoutingKey, Payload}).

setup_exchange(Name, Type) ->
    gen_server:call(?NAME, {setup_exchange, Name, Type}).

stop_exchange(Exchange) ->
    gen_server:call(?NAME, {stop_exchange, Exchange}).

%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

init([]) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, #state{connection = Connection, channel = Channel}}.

handle_call({start_consumer, Exchange, RoutingKey}, _From,
            State = #state{channel = Channel}) ->
    Queue = create_queue(Channel, Exchange, RoutingKey),
    _ConsumerTag = setup_consumer(Channel, Queue, self()),
    {reply, ok, State};
handle_call({setup_exchange, Name, Type}, _From,
            State = #state{channel = Channel}) ->
    Exchange = #'exchange.declare'{exchange = Name, type = Type},
    case amqp_channel:call(Channel, Exchange) of
        #'exchange.declare_ok'{} -> ok;
        _                        -> error
    end,
    {reply, ok, State};
handle_call({stop_exchange, Exchange}, _From,
            State = #state{channel = Channel}) ->
    #'exchange.delete_ok'{} =
        amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange}),
    {reply, ok, State};
handle_call({publish_message, Exchange, RoutingKey, Payload}, _From,
            State = #state{channel = Channel}) ->
    Pub = #'basic.publish'{exchange = Exchange,
                           routing_key = RoutingKey},
    ok = amqp_channel:cast(Channel, Pub, #amqp_msg{payload = Payload}),
    {reply, ok, State};
handle_call(_Request, _From, _State) ->
    not_implemented.

handle_cast(_Request, _State) ->
    not_implemented.

handle_info(#'basic.cancel_ok'{consumer_tag = _ConsumerTag}, State) ->
    % Received when a subscription is cancelled.
    {noreply, State};
handle_info({#'basic.deliver'{consumer_tag = _ConsumerTag,
                             delivery_tag = DeliveryTag},
             #amqp_msg{payload = Payload}},
            State = #state{channel = Channel}) ->
    error_logger:info_msg("Message: ~p", [Payload]),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    {noreply, State};
handle_info(_Message, _State) ->
    not_implemented.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{connection = Connection}) ->
    amqp_connection:close(Connection),
    ok.

create_queue(Channel, Exchange, RoutingKey) ->
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    Binding = #'queue.bind'{queue = Queue,
                            exchange = Exchange,
                            routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    Queue.

setup_consumer(Channel, Queue, ConsumerPid) ->
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, Sub, ConsumerPid),
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
            ok
    end,
    ConsumerTag.
