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

-module(ldb_store).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([start_link/0,
         keys/0,
         get/1,
         update/2,
         update/3,
         update_all/1,
         fold/2]).

%% @doc Returns list of keys.
-callback keys() -> list(key()).

%% @doc Returns the value associated with a given `key()'.
-callback get(key()) -> {ok, value()} | not_found().

%% @doc Applies a given `function()' to a given `key()'.
-callback update(key(), function()) -> ok | not_found() | error().

%% @doc Applies a given `function()' to a given `key()'.
%%      If key not present, use the default `value()'.
-callback update(key(), function(), value()) -> ok | error().

%% @doc Applies a given `function()' to all `key()'s.
-callback update_all(function()) -> ok.

%% @doc Folds the store.
%%      The first argument is the function to be passed to the fold.
%%      The second argument is the initial value for the accumulator.
-callback fold(function(), term()) -> term().

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec keys() -> list(key()).
keys() ->
    do(keys, []).

-spec get(key()) -> {ok, value()} | not_found().
get(Key) ->
    do(get, [Key]).

-spec update(key(), function()) -> ok | not_found() | error.
update(Key, Function) ->
    do(update, [Key, Function]).

-spec update(key(), function(), value()) -> ok | error.
update(Key, Function, Default) ->
    do(update, [Key, Function, Default]).

-spec update_all(function()) -> ok.
update_all(Function) ->
    do(update_all, [Function]).

-spec fold(function(), term()) -> term().
fold(Function, Acc) ->
    do(fold, [Function, Acc]).

%% @private Execute call to the proper store.
do(Function, Args) ->
    Store = ldb_config:get(ldb_store, ?DEFAULT_STORE),
    erlang:apply(Store, Function, Args).
