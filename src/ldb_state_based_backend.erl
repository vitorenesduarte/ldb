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
         update/2,
         message_maker/0,
         message_handler/1,
         memory/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: ldb_node_id()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(key(), type()) -> ok.
create(Key, Type) ->
    gen_server:call(?MODULE, {create, Key, Type}, infinity).

-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

-spec update(key(), operation()) -> ok | not_found() | error().
update(Key, Operation) ->
    gen_server:call(?MODULE, {update, Key, Operation}, infinity).

-spec message_maker() -> function().
message_maker() ->
    fun(Key, CRDT, NodeName) ->
        case ldb_config:get(ldb_driven_mode) of
            none ->
                Message = {Key, state, CRDT},
                {ok, Message};
            state_driven ->
                Actor = ldb_config:id(),
                ShouldStart = Actor < NodeName,
                case ShouldStart of
                    true ->
                        Message = {Key, state_driven, Actor, CRDT},
                        {ok, Message};
                    false ->
                        nothing
                end;
            digest_driven ->
                %% @todo
                nothing
        end
    end.

-spec message_handler(term()) -> function().
message_handler({_, state, _}) ->
    fun({Key, state, {Type, _}=RemoteCRDT}) ->
        create_bottom_entry(Key, Type),
        ldb_store:update(
            Key,
            fun(LocalCRDT) ->
                Merged = Type:merge(LocalCRDT, RemoteCRDT),
                {ok, Merged}
            end
        )
    end;
message_handler({_, state_driven, _, _}) ->
    fun({Key, state_driven, From, {Type, _}=RemoteCRDT}) ->
        create_bottom_entry(Key, Type),
        ldb_store:update(
            Key,
            fun(LocalCRDT) ->
                Delta = Type:delta(state_driven,
                                   LocalCRDT,
                                   RemoteCRDT),
                %% send delta
                Message = {Key, state, Delta},
                ldb_whisperer:send(From, Message),
                Merged = Type:merge(LocalCRDT, Delta),
                {ok, Merged}
            end
        )
    end.

-spec memory() -> {non_neg_integer(), non_neg_integer()}.
memory() ->
    gen_server:call(?MODULE, memory, infinity).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    ?LOG("ldb_state_based_backend initialized!"),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    Type = ldb_util:get_type(LDBType),
    Result = create_bottom_entry(Key, Type),
    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {Type, _}=CRDT} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    Function = fun({Type, _}=CRDT) ->
        Type:mutate(Operation, Actor, CRDT)
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call(memory, _From, State) ->
    FoldFunction = fun({_Key, CRDT}, {C, _}) ->
        CRDTSize = ldb_util:term_size(CRDT),
        {C + CRDTSize, 0}
    end,

    Result = ldb_store:fold(FoldFunction, {0, 0}),
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

%% @private
create_bottom_entry(Key, Type) ->
    %% @todo support complex types
    Result = ldb_store:create(Key, Type:new()),
    Result.
