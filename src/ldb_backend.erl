%%
%% Copyright (c) 2016-2018 Vitor Enes.  All Rights Reserved.
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

-module(ldb_backend).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([start_link/0]).

-export([create/2,
         query/1,
         update/2,
         message_maker/1,
         message_handler/2,
         memory/1,
         backend_state/0]).

%% @doc Create a `key()' in the store with a given `type()'.
-callback create(key(), type()) -> ok.

%% @doc Reads the value associated with a given `key()'.
-callback query(key()) ->
    {ok, value()} | not_found().

%% @doc Update the value associated with a given `key()',
%%      applying a given `operation()'.
-callback update(key(), operation()) ->
    ok | not_found() | error().

%% @doc Returns a function that will, given what's in the store,
%%      decide what should be sent.
%%      The function signature should be:
%%         fun(key(), value(), node_name()) -> Message
%%          - if `Message == nothing', no message is sent.
-callback message_maker(backend_state()) -> function().

%% @doc Returns a function that handles the message received.
-callback message_handler(term(), backend_state()) -> function().

%% @doc Returns memory consumption.
-callback memory(sets:set(string())) -> {size_metric(), size_metric()}.

%% @doc Returns backend config.
-callback backend_state() -> backend_state().

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec create(key(), type()) -> ok.
create(Key, Type) ->
    do(create, [Key, Type]).

-spec query(key()) ->
    {ok, value()} | not_found().
query(Key) ->
    do(query, [Key]).

-spec update(key(), operation()) ->
    ok | not_found() | error().
update(Key, Operation) ->
    do(update, [Key, Operation]).

-spec message_maker(backend_state()) -> function().
message_maker(BackendState) ->
    do(message_maker, [BackendState]).

-spec message_handler(term(), backend_state()) -> function().
message_handler(Message, BackendState) ->
    do(message_handler, [Message, BackendState]).

-spec memory(sets:set(string())) -> {non_neg_integer(), non_neg_integer()}.
memory(IgnoreKeys) ->
    do(memory, [IgnoreKeys]).

-spec backend_state() -> backend_state().
backend_state() ->
    do(backend_state, []).

%% @private Execute call to the proper backend.
do(Function, Args) ->
    Backend = ldb_util:get_backend(),
    erlang:apply(Backend, Function, Args).
