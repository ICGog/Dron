-module(dron_rabbit).
-author("Ionel Corneliu Gog").
-include_lib("amqp_client/include/amqp_client.hrl").

%-------------------------------------------------------------------------------

start() ->
    %% Start connection to the server.
    Connection = amqp_connection:start(#amqp_params_network{}),
    Channel = amqp_connection:open_channel(Connection),
    DeclExch = #'exchange.declare'{exchange = <<"my_exchange">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclExch),
    DeclQueue = #'queue.declare'{queue = <<"my_queue">>},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, DeclQueue).
    
send_message(Channel, Exchange, RoutingKey, Payload) ->
    Pub = #'basic.publish'{exchange = Exchange,
                           routing_key = RoutingKey},
    ok = amqp_channel:cast(Channel, Pub, #amqp_msg{payload = Payload}).

receive_message(Channel, Queue, Tag) ->
    Get = #'basic.get'{queue = Queue},
    {#'basic.get_ok'{delivery_tag = Tag}, Content} =
        amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).

setup_consumer(Channel, Queue, ConsumerPid) ->
    Sub = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, Sub, ConsumerPid).

consume_msgs(Channel, Tag) ->
    receive
        %% First message received.
        #'basic.consume_ok'{} ->
            consume_msgs(Channel, Tag);
        %% Received when the subscription is cancelled.
        #'basic.cancel_ok'{} ->
            ok;
        {#'basic.deliver'{delivery_tag = Tag}, Content} ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
            consume_msgs(Channel, Tag)
    end.

cancel_subscription(Channel, Tag) ->
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}).

close_channel(Channel) ->
    amqp_channel:close(Channel).

stop(Connection) ->
    amqp_connection:close(Connection).
