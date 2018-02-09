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

-module(ldb_actor_store).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_store).
-behaviour(gen_server).

%% ldb_store callbacks
-export([start_link/0,
         keys/0,
         get/1,
         update/2,
         update/3,
         update_all/1,
         fold/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {key_to_data :: maps:map()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec keys() -> list(key()).
keys() ->
    gen_server:call(?MODULE, keys, infinity).

-spec get(key()) -> {ok, value()} | not_found().
get(Key) ->
    gen_server:call(?MODULE, {get, Key}, infinity).

-spec update(key(), function()) -> ok | not_found() | error().
update(Key, Function) ->
    gen_server:call(?MODULE, {update, Key, Function}, infinity).

-spec update(key(), function(), value()) -> ok | error().
update(Key, Function, Default) ->
    gen_server:call(?MODULE, {update, Key, Function, Default}, infinity).

-spec update_all(function()) -> ok.
update_all(Function) ->
    gen_server:call(?MODULE, {update_all, Function}, infinity).

-spec fold(function(), term()) -> term().
fold(Function, Acc) ->
    gen_server:call(?MODULE, {fold, Function, Acc}, infinity).

%% gen_server callbacks
init([]) ->

    lager:info("ldb_actor_store initialized!"),
    {ok, #state{key_to_data=maps:new()}}.

handle_call(keys, _From, #state{key_to_data=Map}=State) ->
    ldb_util:qs("STORE keys"),
    Result = maps:keys(Map),
    {reply, Result, State};

handle_call({get, Key}, _From, #state{key_to_data=Map}=State) ->
    ldb_util:qs("STORE get"),
    Result = do_get(Key, Map),
    {reply, Result, State};

handle_call({update, Key, Function}, _From, #state{key_to_data=Map0}=State) ->
    ldb_util:qs("STORE update/2"),
    {Result, Map} = case do_get(Key, Map0) of
        {ok, Value} ->
            case Function(Value) of
                {ok, NewValue} ->
                    {ok, do_put(Key, NewValue, Map0)};
                Error ->
                    {Error, Map0}
            end;
        Error ->
            {Error, Map0}
    end,

    {reply, Result, State#state{key_to_data=Map}};

handle_call({update, Key, Function, Default}, _From, #state{key_to_data=Map0}=State) ->
    ldb_util:qs("STORE update/3"),

    %% get the current value
    Value = do_get(Key, Map0, Default),

    {Result, Map} = case Function(Value) of
        {ok, NewValue} ->
            {ok, do_put(Key, NewValue, Map0)};
        Error ->
            {Error, Map0}
    end,

    {reply, Result, State#state{key_to_data=Map}};

handle_call({update_all, Function}, _From, #state{key_to_data=Map0}=State) ->
    ldb_util:qs("STORE update_all"),

    Map = maps:map(
        fun(Key, Value) ->
            case Function({Key, Value}) of
                {ok, NewValue} ->
                    %% if okay, update
                    NewValue;
                _ ->
                    %% otherwise, keep the same value
                    Value
            end
        end,
        Map0
    ),

    Result = ok,

    {reply, Result,  State#state{key_to_data=Map}};

handle_call({fold, Function, Acc}, _From, #state{key_to_data=Map}=State) ->
    ldb_util:qs("STORE fold"),
    Result = maps:fold(Function, Acc, Map),
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private Attemts to retrieve a certain value from the map.
do_get(Key, Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            {ok, Value};
        error ->
            {error, not_found}
    end.

%% @private Retrieve a certain value from the map.
do_get(Key, Map, Default) ->
    maps:get(Key, Map, Default).

%% @private Inserts a value in the store (replacing if exists)
do_put(Key, Value, Map) ->
    maps:put(Key, Value, Map).
