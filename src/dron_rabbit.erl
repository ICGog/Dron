-module(dron_rabbit).
-author("Ionel Corneliu Gog").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([start_consumer_link/1, start_producer_link/0, send_message/4]).

-export([start_consumer/1, start_producer/1]).

%-------------------------------------------------------------------------------

start_consumer_link(RoutingKey) ->
    proc_lib:spawn_link(?MODULE, start_consumer, [RoutingKey]).

start_producer_link() ->
    proc_lib:start_link(?MODULE, start_producer, [self()]).
    
%-------------------------------------------------------------------------------
% Internal
%-------------------------------------------------------------------------------

start() ->
    %% Start connection to the server.
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Exchange = #'exchange.declare'{exchange = <<"finished">>,
                                   type = <<"topic">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exchange),
    {Connection, Channel, <<"finished">>}.

start_consumer(RoutingKey) ->
    {_Connection, Channel, Exchange} = start(),
    Queue = create_queue(Channel, Exchange, RoutingKey),
    ConsumerTag = setup_consumer(Channel, Queue, self()),
    consume_msgs(Channel, ConsumerTag).

start_producer(Parent) ->
    proc_lib:init_ack(Parent, start()).

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

send_message(Channel, Exchange, RoutingKey, Payload) ->
    Pub = #'basic.publish'{exchange = Exchange,
                           routing_key = RoutingKey},
    ok = amqp_channel:cast(Channel, Pub, #amqp_msg{payload = Payload}).

consume_msgs(Channel, ConsumerTag) ->
    receive
        %% Received when the subscription is cancelled.
        #'basic.cancel_ok'{consumer_tag = ConsumerTag} ->
            ok;
        {#'basic.deliver'{consumer_tag = ConsumerTag,
                          delivery_tag = DeliveryTag}, Content} ->
            #amqp_msg{payload = Payload} = Content,
            error_logger:info_msg("Message: ~p", [Payload]),
            amqp_channel:cast(
              Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
            consume_msgs(Channel, ConsumerTag)
    end.

stop(Connection, Channel, Exchange) ->
    #'exchange.delete_ok'{} =
        amqp_channel:call(Channel, #'exchange.delete'{exchange = Exchange}),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection).
