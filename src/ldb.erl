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

-module(ldb).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([create/2,
         query/1,
         update/2]).

%% @doc Create a `Key' in the store with a given `Type'.
-spec create(key(), type()) -> ok.
create(Key, Type) ->
    ldb_forward:create(Key, Type).

%% @doc Reads the value associated with a given `Key'.
-spec query(key()) -> {ok, value()}.
query(Key) ->
    ldb_forward:query(Key).

%% @doc Update the value associated with a given `Key',
%%      applying a given `Operation'.
-spec update(key(), operation()) -> ok.
update(Key, Operation) ->
    ldb_forward:update(Key, Operation).
