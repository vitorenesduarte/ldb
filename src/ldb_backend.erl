%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-export([start_link/0,
         create/2,
         query/1,
         update/2,
         prepare_message/3,
         message_handler/1]).

%% @doc Create a `key()' in the store with a given `type()'.
-callback create(key(), type()) -> ok | already_exists().

%% @doc Reads the value associated with a given `key()'.
-callback query(key()) ->
    {ok, value()} | not_found().

%% @doc Update the value associated with a given `key()',
%%      applying a given `operation()'.
-callback update(key(), operation()) ->
    ok | not_found() | error().

%% @doc Given `key()' and the correspondent `value()' in the store,
%%      and a Peer, decide what should be sent.
-callback prepare_message(key(), term(), node_info()) ->
    {ok, term()} | nothing.

%% @doc Returns a function that handles the message received.
-callback message_handler(term()) -> function().

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec create(key(), type()) -> ok | already_exists().
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

-spec prepare_message(key(), term(), node_info()) ->
    {ok, term()} | nothing.
prepare_message(Key, Value, NodeInfo) ->
    do(prepare_message, [Key, Value, NodeInfo]).

-spec message_handler(term()) -> function().
message_handler(Message) ->
    do(message_handler, [Message]).

%% @private Execute call to the proper backend.
do(Function, Args) ->
    Backend = ldb_config:backend(),
    erlang:apply(Backend, Function, Args).
