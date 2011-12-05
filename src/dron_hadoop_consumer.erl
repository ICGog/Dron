-module(dron_hadoop_consumer).
-author("Ionel Corneliu Gog").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([init/3, consume_events/2]).

%-------------------------------------------------------------------------------

init(ParentPid, Channel, Queue) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue},
                               self()),
    proc_lib:init_ack(ParentPid, self()),
    consume_events(Channel, ConsumerTag).

consume_events(Channel, ConsumerTag) ->
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
            consume_events(Channel, ConsumerTag);
        #'basic.cancel_ok'{consumer_tag = ConsumerTag} ->
            error_logger:info_msg("Consumer ~p started", [ConsumerTag]),
            consume_events(Channel, ConsumerTag);
        {#'basic.deliver'{consumer_tag = _ConsumerTag,
                          delivery_tag = DeliveryTag},
         #amqp_msg{payload = Payload}}->
            error_logger:info_msg("Received ~p", [Payload]),
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag}),
            consume_events(Channel, ConsumerTag)
    end.
