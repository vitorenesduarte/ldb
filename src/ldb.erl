%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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

-module(ldb).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([create/2,
         query/1,
         update/2,
         update/3]).

%% @doc Create a `Key' in the store with a given `Type'.
-spec create(key(), type()) -> ok.
create(Key, Type) ->
    ldb_backend:create(Key, Type).

%% @doc Reads the value associated with a given `Key'.
-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    ldb_backend:query(Key).

%% @doc Update the value associated with a given `Key',
%%      applying a given `Operation'.
-spec update(key(), operation()) -> ok | not_found() | error().
update(Key, Operation) ->
    update(node(), Key, Operation).

%% @todo TUTORIAL HACK
update(Actor, Key, Operation) ->
    ldb_backend:update(Actor, Key, Operation).
