-module(dron_event_consumer).
-author("Ionel Corneliu Gog").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([init/3, consume_events/2, get_job_instance/1,
         get_job_instance_state/1]).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Initialize dron_events consumer. It does not return.
%%
%% @spec init(ParentPid, Channel, Queue) -> no
%% @end
%%------------------------------------------------------------------------------
init(ParentPid, Channel, Queue) ->
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue},
                               self()),
    proc_lib:init_ack(ParentPid, self()),
    consume_events(Channel, ConsumerTag).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
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
%            error_logger:info_msg("~p", [Content]),
            JId = get_job_instance(Content),
            case get_job_instance_state(Content) of
                succeeded -> dron_scheduler:job_instance_succeeded(JId);
                failed    -> dron_scheduler:job_instance_failed(
                               JId, get_job_instance_reason(Content));
                timeout   -> dron_scheduler:job_instance_timeout(JId);
                killed    -> dron_scheduler:job_instance_killed(JId)
            end,
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag}),
            consume_events(Channel, ConsumerTag)
    end.

get_job_instance([]) ->
    not_found;
get_job_instance([{<<"job_instance">>, [Host, Year, Month, Day, Hour, Min,
                                        Sec]}|_]) ->
    {binary_to_list(Host), {{Year, Month, Day}, {Hour, Min, Sec}}};
get_job_instance([_|Rest]) ->
    get_job_instance(Rest).

get_job_instance_state([]) ->
    not_found;
get_job_instance_state([{<<"state">>, State}|_]) ->
    list_to_atom(binary_to_list(State));
get_job_instance_state([_|Rest]) ->
    get_job_instance_state(Rest).

get_job_instance_reason([]) ->
    job_reported_failed;
get_job_instance_reason([{<<"reason">>, Reason}|_]) ->
    list_to_atom(binary_to_list(Reason));
get_job_instance_reason([_|Rest]) ->
    get_job_instance_reason(Rest).
