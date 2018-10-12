%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(ldb_forward).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([all_shards/0,
         create/2,
         query/2,
         update/2,
         update_members/1,
         update_ignore_keys/1,
         get_metrics/0]).

%% number of shards
-define(SHARD_NUMBER, 2).

%% @doc
-spec all_shards() -> list(atom()).
all_shards() ->
    lists:map(
        fun(Index) -> ldb_util:integer_to_atom(Index) end,
        lists:seq(0, ?SHARD_NUMBER - 1)
    ).

%% @doc Create a `Key' in the store with a given `Type'.
-spec create(key(), type()) -> ok.
create(Key, Type) ->
    forward(Key, call, {create, Key, Type}).

%% @doc Reads the value associated with a given `Key'.
-spec query(key(), list(term())) -> {ok, value()}.
query(Key, Args) ->
    forward(Key, call, {query, Key, Args}).

%% @doc Update the value associated with a given `Key',
%%      applying a given `Operation'.
-spec update(key(), operation()) -> ok.
update(Key, Operation) ->
    forward(Key, call, {update, Key, Operation}).

%% @doc Update members.
-spec update_members(list(ldb_node_id())) -> ok.
update_members(Members) ->
    forward(all, cast, {update_members, Members}).

%% @doc Update ignore keys.
-spec update_ignore_keys(sets:set(string())) -> ok.
update_ignore_keys(IgnoreKeys) ->
    forward(all, call, {update_ignore_keys, IgnoreKeys}).

%% @doc Get metrics from all shards.
-spec get_metrics() -> list(metrics()).
get_metrics() ->
    forward(all, call, get_metrics).

%% @private
-spec forward(key() | all, atom(), term()) -> term().
forward(all, What, Msg) ->
    lists:map(
        fun(Name) -> do_forward(Name, What, Msg) end,
        all_shards()
    );
forward(Key, What, Msg) ->
    do_forward(shard(Key), What, Msg).

%% @private Forward by call or cast.
-spec do_forward(atom(), atom(), term()) -> term().
do_forward(Name, call, Msg) ->
    gen_server:call(Name, Msg, infinity);
do_forward(Name, cast, Msg) ->
    gen_server:cast(Name, Msg).

%% @private Get shard name.
-spec shard(term()) -> atom().
shard(Key) ->
    Index = erlang:phash2(Key, ?SHARD_NUMBER),
    ldb_util:integer_to_atom(Index).
