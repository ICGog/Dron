-module(dron_db).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([get_new_id/0]).

%-------------------------------------------------------------------------------

get_new_id() ->
    mnesia:dirty_update_counter(ids, id, 1).
