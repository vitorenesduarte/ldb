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

-module(ldb_store).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([
         start_link/0,
         get/1,
         put/2
        ]).

%% @doc Returns the value associated with a given `key()'.
-callback get(key()) -> {ok, value()} | {error, not_found}.

%% @doc Updates a given `key()' with a given `value()'.
-callback put(key(), value()) -> ok.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec get(key()) -> {ok, value()} | {error, not_found}.
get(Key) ->
    do(get, [Key]).

-spec put(key(), value()) -> ok.
put(Key, Value) ->
    do(put, [Key, Value]).

%% @private Execute call to the proper store.
do(Function, Args) ->
    Store = ldb_util:store(),
    erlang:apply(Store, Function, Args).
