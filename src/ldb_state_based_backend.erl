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
%% @todo TUTORIAL HACK
         update/3,
         message_maker/0,
         message_handler/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: non_neg_integer()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(key(), type()) -> ok.
create(Key, Type) ->
    gen_server:call(?MODULE, {create, Key, Type}, infinity).

-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

%% @todo TUTORIAL HACK
-spec update(term(), key(), operation()) -> ok | not_found() | error().
update(Actor, Key, Operation) ->
    gen_server:call(?MODULE, {update, Actor, Key, Operation}, infinity).

-spec message_maker() -> function().
message_maker() ->
    fun(Key, CRDT, _NodeName) ->
        Message = {Key, state_send, CRDT},
        {ok, Message}
    end.

-spec message_handler(term()) -> function().
message_handler(_Message) ->
    fun({Key, state_send, {Type, _}=RemoteCRDT}) ->
        %% Create a bottom entry (if not present)
        %% @todo support complex types
        _ = ldb_store:create(Key, Type:new()),
        ldb_store:update(
            Key,
            fun(LocalCRDT) ->
                Merged = Type:merge(LocalCRDT, RemoteCRDT),
                {ok, Merged}
            end
        )
    end.

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    ldb_log:info("ldb_state_based_backend initialized!"),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    Type = ldb_util:get_type(LDBType),
    %% @todo support complex types
    Result = ldb_store:create(Key, Type:new()),
    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {Type, _}=CRDT} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

%% @todo TUTORIAL HACK
handle_call({update, Actor, Key, Operation}, _From, #state{actor=_Actor}=State) ->
    Function = fun({Type, _}=CRDT) ->
        Type:mutate(Operation, Actor, CRDT)
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
