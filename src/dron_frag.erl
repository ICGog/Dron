-module(dron_frag).
-author("Ionel Corneliu Gog").
-include("dron.hrl").
-behaviour(mnesia_frag_hash).

-export([init_state/2, add_frag/1, del_frag/1, key_to_frag_number/2,
         match_spec_to_frag_numbers/2, hash/2]).

-record(hash_state, {n_fragments}).

%===============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @spec init_state(Tab, State) -> NewState | abort(Reason)
%% @end
%%------------------------------------------------------------------------------
init_state(Tab, State) when is_atom(Tab), State == undefined ->
  #hash_state{n_fragments = 1}.

%%------------------------------------------------------------------------------
%% @doc
%% @spec add_frag(State) -> {NewState, IterFrags, AdditionalLockFrags} |
%%                          abort(Reason)
%% @end
%%------------------------------------------------------------------------------
add_frag(#hash_state{n_fragments = NumFrag} = State) ->
  {State#hash_state{n_fragments = NumFrag + 1}, [], [NumFrag + 1]}.

%%------------------------------------------------------------------------------
%% @doc
%% @spec del_frag(State) -> {NewState, IterFrags, AdditionalLockFrags} |
%%                          abort(Reason)
%% @end
%%------------------------------------------------------------------------------
del_frag(#hash_state{n_fragments = NumFrag} = State) ->
  {State#hash_state{n_fragments = NumFrag - 1}, [NumFrag], []}.

%%------------------------------------------------------------------------------
%% @doc
%% The function is invoked whenever Mnesia needs to determine where a certain
%% record is stored.
%%
%% @spec key_to_frag_number(State, Key) -> FragNum | abort(Reason)
%% @end
%%------------------------------------------------------------------------------
key_to_frag_number(#hash_state{n_fragments = NumFrag} , Key) ->
  hash(Key, NumFrag).

%%------------------------------------------------------------------------------
%% @doc
%% The function returns the fragments that must be searched when a match_object,
%% select is conducted.
%%
%% @spec match_spec_to_frag_numbers(State, MatchSpec) -> FragNums |
%%                                                       abort(Reason)
%% @end
%%------------------------------------------------------------------------------
match_spec_to_frag_numbers(_State, _MatchSpec) ->
  [1].

%%------------------------------------------------------------------------------
%% @doc
%% Hash a key to a fragment.
%%
%% @spec hash(Key, NumFrag) -> FragNumber
%% @end
%%------------------------------------------------------------------------------
hash(_Key, _NumFrag) ->
  1.
