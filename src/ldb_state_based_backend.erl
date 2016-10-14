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

-module(ldb_state_based_backend).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: atom()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(key(), type()) ->
    ok | {error, key_already_existing_with_different_type}.
create(Key, Type) ->
    gen_server:call(?MODULE, {create, Key, Type}, infinity).

-spec query(key()) -> {ok, value()} | {error, not_found}.
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

-spec update(key(), operation()) -> ok | {error, not_found} | error().
update(Key, Operation) ->
    gen_server:call(?MODULE, {update, Key, Operation}, infinity).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = node(),

    lager:info("ldb_state_based_backend initialized!"),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, Type}, _From, State) ->
    ActualType = ldb_util:get_type(Type),

    Result = case ldb_store:get(Key) of
        {ok, {KeyType, _Value}} ->
            case KeyType of
                ActualType ->
                    %% already created with the same type
                    ok;
                _ ->
                    %% already created with a different type
                    {error, key_already_existing_with_different_type}
            end;
        {error, not_found} ->
            %% @todo support complex types
            New = ActualType:new(),
            ldb_store:put(Key, {ActualType, New}),
            ok
    end,

    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {ActualType, Value}} ->
            ActualType:query(Value);
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    Result = case ldb_store:get(Key) of
        {ok, {ActualType, Value}} ->
            case ActualType:mutate(Operation, Actor, Value) of
                {ok, NewValue} ->
                    ldb_store:put(Key, {ActualType, NewValue}),
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
