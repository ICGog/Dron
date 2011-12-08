-module(dron_event_consumer).
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
            {struct, Content} = mochijson2:decode(binary_to_list(Payload)),
            JobInstance = get_job_instance(Content),
            JobInstanceState = get_job_instance_state(Content),
            error_logger:info_msg("Received ~p ~p",
                                  [JobInstance, JobInstanceState]),
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag}),
            consume_events(Channel, ConsumerTag)
    end.

get_job_instance([]) ->
    not_found;
get_job_instance([{<<"job_instance">>, JobInstance}|Rest]) ->
    JobInstance;
get_job_instance([_|Rest]) ->
    get_job_instance(Rest).

get_job_instance_state([]) ->
    not_found;
get_job_instance_state([{<<"state">>, State}|Rest]) ->
    binary_to_list(State);
get_job_instance_state([_|Rest]) ->
    get_job_instance_state(Rest).
