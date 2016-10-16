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

-module(ldb_state_based_backend).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2,
         prepare_message/2,
         message_handler/1]).

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

-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

-spec update(key(), operation()) -> ok | not_found() | error().
update(Key, Operation) ->
    gen_server:call(?MODULE, {update, Key, Operation}, infinity).

-spec prepare_message(key(), term()) -> term().
prepare_message(Key, Value) ->
    {Key, Value}.

-spec message_handler(term()) -> function().
message_handler(_Message) ->
    %% @todo The key may not be present yet.
    MessageHandler = fun({Key, {_, Value}}) ->
        ldb_store:update(
            Key,
            fun({ActualType, V}) ->
                Merged = ActualType:merge(Value, V),
                {ok, {ActualType, Merged}}
            end
        )
    end,
    MessageHandler.

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
            {ok, ActualType:query(Value)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    Function = fun({ActualType, Value}) ->
        case ActualType:mutate(Operation, Actor, Value) of
            {ok, NewValue} ->
                {ok, {ActualType, NewValue}};
            Error ->
                Error
        end
    end,

    Result = case ldb_store:update(Key, Function) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end,
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
