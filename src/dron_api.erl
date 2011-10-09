-module(dron_api).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([register_job/1, unregister_job/1]).

%-------------------------------------------------------------------------------

register_job(Job) ->
    mnesia:write(Job),
    dron_scheduler:schedule(Job).

unregister_job(Job) ->
    dron_scheduler:unschedule(Job),
    mnesia:delete_object(Job).
    
