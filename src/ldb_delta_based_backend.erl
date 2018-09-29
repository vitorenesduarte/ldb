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

-module(ldb_delta_based_backend).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2,
         update_members/1,
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

-spec update_members(list(ldb_node_id())) -> ok.
update_members(Members) ->
    gen_server:cast(?MODULE, {update_members, Members}).

-spec message_maker() -> function().
message_maker() ->
    fun(Key, {{Type, _}=CRDT, Sequence, DeltaBuffer, AckMap}, NodeName) ->

        %% config
        Actor = ldb_config:id(),
        Mode = ldb_config:get(ldb_driven_mode),
        BP = ldb_config:get(ldb_dgroup_back_propagation, false),

        MinSeq = min_seq_buffer(DeltaBuffer),
        LastAck = last_ack(NodeName, AckMap),

        case LastAck < Sequence of
            true ->
                ToSend = case orddict:is_empty(DeltaBuffer) orelse MinSeq > LastAck of
                    true ->
                        ShouldStart = Actor < NodeName,

                        case Mode of
                            none ->
                                {
                                    Key,
                                    delta,
                                    Actor,
                                    Sequence,
                                    CRDT
                                };
                            _ ->
                                case ShouldStart of
                                    true ->
                                        %% compute bottom
                                        Bottom = ldb_util:new_crdt(state, CRDT),

                                        %% compute digest
                                        Digest = case Mode of
                                            state_driven ->
                                                {state, CRDT};
                                            digest_driven ->
                                                %% this can still be a CRDT state
                                                %% if implemented like that
                                                %% by the data type
                                                Type:digest(CRDT)
                                        end,

                                        {
                                            Key,
                                            digest,
                                            Actor,
                                            Sequence,
                                            Bottom,
                                            Digest
                                        };
                                    false ->
                                        nothing
                                end
                        end;
                    false ->
                        DeltaGroup = orddict:fold(
                            fun(N, {From, D}, Acc) ->
                                ShouldSendDelta0 = LastAck =< N andalso N < Sequence,
                                ShouldSendDelta1 = case BP of
                                    true ->
                                        % when set to true, avoids back propagation of delta groups
                                        ShouldSendDelta0 andalso NodeName =/= From;
                                    false ->
                                        ShouldSendDelta0
                                end,

                                case ShouldSendDelta1 of
                                    true ->
                                        Type:merge(D, Acc);
                                    false ->
                                        Acc
                                end
                            end,
                            ldb_util:new_crdt(state, CRDT),
                            DeltaBuffer
                        ),

                        case Type:is_bottom(DeltaGroup) of
                            true ->
                                nothing;
                            false ->
                                {
                                    Key,
                                    delta,
                                    Actor,
                                    Sequence,
                                    DeltaGroup
                                }
                        end
                end,

                ToSend;
            false ->
                nothing
        end
    end.

-spec message_handler(term()) -> function().
message_handler({_, delta, _, _, _}) ->
    fun({Key, delta, From, N, {Type, _}=RemoteCRDT}) ->

        %% config
        Actor = ldb_config:id(),
        RR = ldb_config:get(ldb_redundant_dgroups, false),

        %% create bottom entry
        Bottom = ldb_util:new_crdt(state, RemoteCRDT),
        Default = get_entry(Bottom),

        ldb_store:update(
            Key,
            fun({LocalCRDT0, Sequence0, DeltaBuffer0, AckMap}) ->

                {Delta, LocalCRDT} = Type:delta_and_merge(RemoteCRDT, LocalCRDT0),
                {Sequence, DeltaBuffer} = case not Type:is_bottom(Delta) of
                    true ->
                        %% If what we received, inflates the local state
                        ToStore = case RR of
                            true -> Delta;
                            false -> RemoteCRDT
                        end,
                        {Sequence0 + 1, orddict:store(Sequence0, {From, ToStore}, DeltaBuffer0)};
                    false ->
                        {Sequence0, DeltaBuffer0}
                end,

                %% send ack
                Ack = {
                    Key,
                    delta_ack,
                    Actor,
                    N
                },
                ldb_whisperer:send(From, Ack),

                StoreValue = {LocalCRDT, Sequence, DeltaBuffer, AckMap},
                {ok, StoreValue}
            end,
            Default
        )
    end;
message_handler({_, delta_ack, _, _}) ->
    fun({Key, delta_ack, From, N}) ->
        ldb_store:update(
            Key,
            fun({LocalCRDT, Sequence, DeltaBuffer0, AckMap0}) ->
                LastAck = last_ack(From, AckMap0),

                %% when a new ack is received,
                %% update the number of rounds without
                %% receiving an ack to 0
                MaxAck = max(LastAck, N),
                AckMap1 = maps:put(From, MaxAck, AckMap0),

                %% and try to shrink the delta-buffer immediately
                DeltaBuffer1 = buffer_shrink(min_seq_ack_map(AckMap1), DeltaBuffer0),

                StoreValue = {LocalCRDT, Sequence, DeltaBuffer1, AckMap1},
                {ok, StoreValue}
            end
        )
    end;
message_handler({_, digest, _, _, _, _}) ->
    fun({Key, digest, From, RemoteSequence, {Type, _}=Bottom, Remote}) ->

        %% config
        Actor = ldb_config:id(),

        Default = get_entry(Bottom),
        {ok, {LocalCRDT, LocalSequence, _, _}} = ldb_store:update(
            Key,
            fun(V) -> {ok, V} end,
            Default
        ),

        %% compute delta
        Delta = Type:delta(LocalCRDT, Remote),

        ToSend = case Remote of
            {state, RemoteCRDT} ->

                FakeMessage = {
                    Key,
                    delta,
                    From,
                    RemoteSequence,
                    RemoteCRDT
                },
                Handler = message_handler(FakeMessage),
                Handler(FakeMessage),

                %% send delta
                {
                    Key,
                    delta,
                    Actor,
                    LocalSequence,
                    Delta
                };
            {mdata, _} ->

                LocalDigest = Type:digest(LocalCRDT),

                %% send delta and digest
                {
                    Key,
                    digest_and_state,
                    Actor,
                    LocalSequence,
                    Delta,
                    LocalDigest
                }
        end,

        ldb_whisperer:send(From, ToSend)
    end;
message_handler({_, digest_and_state, _, _, _, _}) ->
    fun({Key, digest_and_state, From, RemoteSequence,
         {Type, _}=RemoteDelta, RemoteDigest}) ->

        %% config
        Actor = ldb_config:id(),

        {ok, {LocalCRDT, LocalSequence, _, _}} = ldb_store:get(Key),

        %% compute delta
        Delta = Type:delta(LocalCRDT, RemoteDigest),

        Message = {
            Key,
            delta,
            Actor,
            LocalSequence,
            Delta
        },

        %% send delta
        ldb_whisperer:send(From, Message),

        FakeMessage = {
            Key,
            delta,
            From,
            RemoteSequence,
            RemoteDelta
        },
        Handler = message_handler(FakeMessage),
        Handler(FakeMessage)

    end.

-spec memory() -> {size_metric(), size_metric()}.
memory() ->
    gen_server:call(?MODULE, memory, infinity).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    %% configure members callback
    MembersFun = fun(Membership) ->
        Members = ldb_util:parse_membership(Membership),
        ldb_delta_based_backend:update_members(Members)
    end,
    partisan_peer_service:add_sup_callback(MembersFun),

    lager:info("ldb_delta_based_backend initialized!"),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    ldb_util:qs("DELTA BACKEND create"),
    Bottom = ldb_util:new_crdt(type, LDBType),
    Default = get_entry(Bottom),

    Result = ldb_store:update(
        Key,
        fun(V) -> {ok, V} end,
        Default
    ),

    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    ldb_util:qs("DELTA BACKEND query"),
    Result = case ldb_store:get(Key) of
        {ok, {{Type, _}=CRDT, _, _, _}} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    ldb_util:qs("DELTA BACKEND update"),
    Function = fun({{Type, _}=CRDT0, Sequence, DeltaBuffer0, AckMap}) ->
        case Type:delta_mutate(Operation, Actor, CRDT0) of
            {ok, Delta} ->
                CRDT1 = Type:merge(Delta, CRDT0),
                DeltaBuffer1 = orddict:store(Sequence, {Actor, Delta}, DeltaBuffer0),
                StoreValue = {CRDT1, Sequence + 1, DeltaBuffer1, AckMap},
                {ok, StoreValue};
            Error ->
                Error
        end
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call(memory, _From, State) ->
    ldb_util:qs("DELTA BACKEND memory"),
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

handle_cast({update_members, Peers}, State) ->
    ldb_util:qs("DELTA BACKEND update_members"),

    ShrinkFun = fun({_Key, {LocalCRDT, Sequence, DeltaBuffer0, AckMap0}}) ->
        %% only keep in the ack map entries from current peers
        AckMap1 = maps:with(Peers, AckMap0),

        %% ensure all current peers have an entry in the ack map
        AllPeersInAckMap = lists:all(
            fun(Peer) ->
                maps:is_key(Peer, AckMap1)
            end,
            Peers
        ),

        %% if all peers are in the ack map,
        %% remove from the delta buffer all the entries
        %% acknowledged by all the peers
        DeltaBuffer1 = case AllPeersInAckMap of
            true ->
                Min = case length(Peers) of
                    0 ->
                        %% if no peers, delete all entries
                        %% in the delta buffer
                        Sequence;
                    _ ->
                        min_seq_ack_map(AckMap1)
                end,

                buffer_shrink(Min, DeltaBuffer0);
            false ->
                DeltaBuffer0
        end,

        ?DEBUG("Delta-buffer size before/after: ~p/~p",
               [orddict:size(DeltaBuffer0),
                orddict:size(DeltaBuffer1)]),

        NewValue = {LocalCRDT, Sequence, DeltaBuffer1, AckMap1},
        {ok, NewValue}
    end,

    ldb_store:update_all(ShrinkFun),
    {noreply, State};

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
    Sequence = 0,
    DeltaBuffer = orddict:new(),
    AckMap = maps:new(),

    {Bottom, Sequence, DeltaBuffer, AckMap}.

%% @private Use the fact that the buffer is ordered.
min_seq_buffer([]) -> 0;
min_seq_buffer([{Seq, _}|_]) -> Seq.

%% @private
min_seq_ack_map(AckMap) ->
    lists:min(maps:values(AckMap)).

%% @private
last_ack(NodeName, AckMap) ->
    maps:get(NodeName, AckMap, 0).

%% @private Drop all entries with `Seq < Min'.
buffer_shrink(Min, [{Seq, _}|T]) when Seq < Min ->
    buffer_shrink(Min, T);
buffer_shrink(_, L) ->
    L.
