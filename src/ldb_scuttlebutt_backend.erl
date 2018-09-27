%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
%%
%% Version 2.0 (the "License"); you may not use this file
%% This file is provided to you under the Apache License,
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

-module(ldb_scuttlebutt_backend).
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
    fun(Key, {CRDT, _VV, _DeltaBuffer, Matrix}, _NodeName) ->
        
        %% config
        Actor = ldb_config:id(),

        %% compute bottom
        Bottom = ldb_util:new_crdt(state, CRDT),

        %% message
        {
            Key,
            digest,
            Actor,
            Bottom,
            m_vclock:matrix(Matrix)
        }
    end.

-spec message_handler(term()) -> function().
message_handler({_, digest, _, _, _}) ->
    fun({Key, digest, From, Bottom, RemoteMatrix}) ->
        
        %% store it, in case it's new
        Default = get_entry(Bottom),
        ldb_store:update(
            Key,
            fun({CRDT, VV, DeltaBuffer0, Matrix0}) ->
                Matrix1 = m_vclock:union_matrix(Matrix0, RemoteMatrix),

                %% get new stable dots
                {StableDots, Matrix2} = m_vclock:stable(Matrix1),

                %% prune these stable dots
                DeltaBuffer1 = lists:foldl(
                    fun(StableDot, Acc) -> maps:remove(StableDot, Acc) end,
                    DeltaBuffer0,
                    StableDots
                ),
                {ok, {CRDT, VV, DeltaBuffer1, Matrix2}}
            end,
            Default
        ),

        %% TODO this could be optimized if ldb_store:update
        %% would return the current value
        %% - this pattern seems to occur in other backends as well
        {ok, {_, DeltaBuffer, _, _}} = ldb_store:get(Key),

        %% extract remote vv
        RemoteVV = maps:get(From, RemoteMatrix),

        %% find dots that do not exist in the remote node
        Result = maps:fold(
            fun(Dot, Delta, Acc) ->
                case vclock:is_element(Dot, RemoteVV) of
                    true -> Acc;
                    false -> [{Dot, Delta} | Acc]
                end
            end,
            [],
            DeltaBuffer
        ),
        
        %% send buffer
        Message = {
            Key,
            buffer,
            Result
        },
        ldb_whisperer:send(From, Message)

    end;
message_handler({_, buffer, _}) ->
    fun({Key, buffer, Buffer}) ->
        ldb_store:update(
            Key,
            fun({CRDT0, VV, DeltaBuffer0, Matrix}) ->

                {CRDT1, DeltaBuffer1} = lists:foldl(
                    fun({Dot, Delta}, {CRDTAcc, DeltaBufferAcc}) ->
                        store_delta(Dot, Delta, CRDTAcc, DeltaBufferAcc)
                    end,
                    {CRDT0, DeltaBuffer0},
                    Buffer
                ),

                StoreValue = {CRDT1, VV, DeltaBuffer1, Matrix},
                {ok, StoreValue}
            end
        )
    end.

-spec memory() -> {size_metric(), size_metric()}.
memory() ->
    gen_server:call(?MODULE, memory, infinity).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    lager:info("ldb_scuttlebutt_backend initialized!"),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    ldb_util:qs("SCUTTLEBUTT BACKEND create"),
    Bottom = ldb_util:new_crdt(type, LDBType),
    Default = get_entry(Bottom),

    Result = ldb_store:update(
        Key,
        fun(V) -> {ok, V} end,
        Default
    ),

    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    ldb_util:qs("SCUTTLEBUTT BACKEND query"),
    Result = case ldb_store:get(Key) of
        {ok, {{Type, _}=CRDT, _, _, _}} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    ldb_util:qs("SCUTTLEBUTT BACKEND update"),
    Function = fun({{Type, _}=CRDT0, VV0, DeltaBuffer0, Matrix0}) ->
        case Type:delta_mutate(Operation, Actor, CRDT0) of
            {ok, Delta} ->
                {Dot, VV1} = vclock:next_dot(Actor, VV0),
                Matrix1 = m_vclock:update(Actor, VV1, Matrix0),
                {CRDT1, DeltaBuffer1} = store_delta(Dot, Delta, CRDT0, DeltaBuffer0),
                StoreValue = {CRDT1, VV1, DeltaBuffer1, Matrix1},
                {ok, StoreValue};
            Error ->
                Error
        end
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call(memory, _From, State) ->
    ldb_util:qs("SCUTTLEBUTT BACKEND memory"),
    FoldFunction = fun(_Key, Value, {C0, R0}) ->
        {CRDT, _Sequence, DeltaBuffer, AckMap} = Value,

        C = ldb_util:plus(C0, ldb_util:size(crdt, CRDT)),

        %% delta buffer + ack map
        R = ldb_util:plus([
            R0,
            ldb_util:size(ack_map, AckMap),
            ldb_util:size(delta_buffer, DeltaBuffer)
        ]),

        {C, R}
    end,

    Result = ldb_store:fold(FoldFunction, {{0, 0}, {0, 0}}),
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
get_entry(Bottom) ->
    NodeNumber = ldb_config:get(node_number),
    VV = vclock:new(),
    DeltaBuffer = maps:new(),
    Matrix = m_vclock:new(NodeNumber),
    {Bottom, VV, DeltaBuffer, Matrix}.

%% @private
store_delta(Dot, Delta, {Type, _}=CRDT0, DeltaBuffer0) ->
    CRDT1 = Type:merge(CRDT0, Delta),
    DeltaBuffer1 = maps:put(Dot, Delta, DeltaBuffer0),
    {CRDT1, DeltaBuffer1}.
