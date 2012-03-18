-module(dron_hash).
-author("Ionel Corneliu Gog").
-include("dron.hrl").

-export([hash/2]).

%===============================================================================

hash(JName, Nodes) ->
  lists:nth(erlang:phash(JName, length(Nodes)), Nodes).
