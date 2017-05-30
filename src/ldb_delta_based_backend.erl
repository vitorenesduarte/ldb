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
    fun(Key, {{Type, _}=CRDT, Sequence, DeltaBuffer, AckMap0}=Value, NodeName) ->
        Actor = ldb_config:id(),
        MinSeq = min_seq_buffer(DeltaBuffer),
        {LastAck, _} = last_ack(NodeName, AckMap0),

        Result = case LastAck < Sequence of
            true ->
                {ToSend, AckMap1} = case orddict:is_empty(DeltaBuffer) orelse MinSeq > LastAck of
                    true ->
                        ShouldStart = Actor < NodeName,

                        Mode = ldb_config:get(ldb_driven_mode),

                        case Mode of
                            none ->
                                Message = {
                                    Key,
                                    delta,
                                    Actor,
                                    Sequence,
                                    CRDT
                                },
                                {Message, AckMap0};
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

                                        Message = {
                                            Key,
                                            digest,
                                            Actor,
                                            Sequence,
                                            Bottom,
                                            Digest
                                        },

                                        {Message, AckMap0};
                                    false ->
                                        {nothing, AckMap0}
                                end
                        end;
                    false ->
                        DeltaGroup = orddict:fold(
                            fun(N, {From, D}, Acc) ->
                                ShouldSendDelta0 = LastAck =< N andalso N < Sequence,
                                ShouldSendDelta1 = case ldb_config:get(ldb_dgroup_back_propagation, false) of
                                    true ->
                                        % when set to true, avoids back propagation of delta groups
                                        ShouldSendDelta0 andalso NodeName =/= From;
                                    false ->
                                        ShouldSendDelta0
                                end,

                                case ShouldSendDelta1 of
                                    true ->
                                        Type:merge(Acc, D);
                                    false ->
                                        Acc
                                end
                            end,
                            ldb_util:new_crdt(state, CRDT),
                            DeltaBuffer
                        ),

                        Message = case Type:is_bottom(DeltaGroup) of
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
                        end,

                        {Message, increment_ack_map_round(NodeName, AckMap0)}
                end,

                %% update the ack map
                NewValue = {CRDT, Sequence, DeltaBuffer, AckMap1},

                {ToSend, NewValue};
            false ->
                {nothing, Value}
        end,

        Result
    end.

-spec message_handler(term()) -> function().
message_handler({_, delta, _, _, _}) ->
    fun({Key, delta, From, N, {Type, _}=RemoteCRDT}) ->

        %% create bottom entry
        Bottom = ldb_util:new_crdt(state, RemoteCRDT),
        Default = get_entry(Bottom),

        ldb_store:update(
            Key,
            fun({LocalCRDT, Sequence0, DeltaBuffer0, AckMap}) ->
                Merged = Type:merge(LocalCRDT, RemoteCRDT),

                {Sequence, DeltaBuffer} = case ldb_config:get(ldb_redundant_dgroups, false) of
                    true ->
                        Delta = Type:delta(RemoteCRDT, {state, LocalCRDT}),

                        %% If what we received, inflates the local state
                        case not Type:is_bottom(Delta) of
                            true ->
                                DeltaBuffer1 = orddict:store(Sequence0, {From, Delta}, DeltaBuffer0),
                                Sequence1 = Sequence0 + 1,
                                {Sequence1, DeltaBuffer1};
                            false ->
                                {Sequence0, DeltaBuffer0}
                        end;
                    false ->

                        %% If what we received, inflates the local state
                        case Type:is_strict_inflation(LocalCRDT, Merged) of
                            true ->
                                DeltaBuffer1 = orddict:store(Sequence0, {From, RemoteCRDT}, DeltaBuffer0),
                                Sequence1 = Sequence0 + 1,
                                {Sequence1, DeltaBuffer1};
                            false ->
                                {Sequence0, DeltaBuffer0}
                        end
                end,

                %% send ack
                Ack = {
                    Key,
                    delta_ack,
                    ldb_config:id(),
                    N
                },
                ldb_whisperer:send(From, Ack),

                StoreValue = {Merged, Sequence, DeltaBuffer, AckMap},
                {ok, StoreValue}
            end,
            Default
        )
    end;
message_handler({_, delta_ack, _, _}) ->
    fun({Key, delta_ack, From, N}) ->
        ldb_store:update(
            Key,
            fun({LocalCRDT, Sequence, DeltaBuffer, AckMap0}) ->
                {LastAck, Round} = last_ack(From, AckMap0),

                %% when a new ack is received,
                %% update the number of rounds without
                %% receiving an ack to 0
                MaxAck = max(LastAck, N),
                NewRound = case MaxAck > LastAck of
                    true ->
                        0;
                    false ->
                        Round
                end,

                AckMap1 = orddict:store(From, {MaxAck, NewRound}, AckMap0),
                StoreValue = {LocalCRDT, Sequence, DeltaBuffer, AckMap1},
                {ok, StoreValue}
            end
        )
    end;
message_handler({_, digest, _, _, _, _}) ->
    fun({Key, digest, From, RemoteSequence, {Type, _}=Bottom, Remote}) ->
        Default = get_entry(Bottom),
        ldb_store:update(
            Key,
            fun(V) -> {ok, V} end,
            Default
        ),

        {ok, {LocalCRDT, LocalSequence, _, _}} = ldb_store:get(Key),
        Actor = ldb_config:id(),

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

        {ok, {LocalCRDT, LocalSequence, _, _}} = ldb_store:get(Key),
        Actor = ldb_config:id(),

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

-spec memory() -> {non_neg_integer(), non_neg_integer()}.
memory() ->
    gen_server:call(?MODULE, memory, infinity).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    schedule_dbuffer_shrink(),

    ?LOG("ldb_delta_based_backend initialized!"),
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
                CRDT1 = Type:merge(CRDT0, Delta),
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
    FoldFunction = fun({_Key, Value}, {C, R}) ->
        {CRDT, Sequence, DeltaBuffer, AckMap} = Value,
        CRDTSize = ldb_util:size(crdt, CRDT),
        RestSize = ldb_util:size(term, {Sequence, AckMap})
                 + ldb_util:size(delta_buffer, DeltaBuffer),
        {C + CRDTSize, R + RestSize}
    end,

    Result = ldb_store:fold(FoldFunction, {0, 0}),
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(dbuffer_shrink, State) ->
    ldb_util:qs("DELTA BACKEND dbuffer_shrink"),

    Peers = ldb_whisperer:members(),

    ShrinkFun = fun({LocalCRDT, Sequence, DeltaBuffer0, AckMap0}) ->
        %% only keep in the ack map entries from current peers
        AckMap1 = [Entry || {Peer, _}=Entry <- AckMap0, lists:member(Peer, Peers)],

        %% ensure all current peers have an entry in the ack map
        AllPeersInAckMap = lists:all(
            fun(Peer) ->
                orddict:is_key(Peer, AckMap1)
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
                        0;
                    _ ->
                        min_seq_ack_map(AckMap1)
                end,

                orddict:filter(
                    fun(EntrySequence, {_Actor, _Delta}) ->
                        EntrySequence >= Min
                    end,
                    DeltaBuffer0
                );
            false ->
                DeltaBuffer0
        end,

        lager:info("DBUFFER SIZE BEFORE/AFTER: ~p/~p\n\n", [orddict:size(DeltaBuffer0), orddict:size(DeltaBuffer1)]),

        NewValue = {LocalCRDT, Sequence, DeltaBuffer1, AckMap1},
        {ok, NewValue}
    end,

    ldb_store:update_all(ShrinkFun),

    schedule_dbuffer_shrink(),
    {noreply, State};

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
    AckMap = orddict:new(),

    {Bottom, Sequence, DeltaBuffer, AckMap}.

%% @private
min_seq_buffer(DeltaBuffer) ->
    case orddict:fetch_keys(DeltaBuffer) of
        [] ->
            0;
        Keys ->
            lists:nth(1, Keys)
    end.

%% @private
min_seq_ack_map(AckMap) ->
    lists:min([Ack || {_, {Ack, _Round}} <- AckMap]).

%% @private
last_ack(NodeName, AckMap) ->
    orddict_ext:fetch(NodeName, AckMap, {0, 0}).

%% @private
increment_ack_map_round(NodeName, AckMap) ->
    {LastAck, Round} = last_ack(NodeName, AckMap),
    NextRound = Round + 1,

    EvictionRoundNumber = eviction_round_number(),

    %% if eviction round number is bigger than the current round number
    %% and we should evict peers from the delta buffer
    %% (i.e., eviction round number != -1),
    %% evict peer from the ack map
    case NextRound > EvictionRoundNumber andalso EvictionRoundNumber /= -1 of
        true ->
            lager:info("NODE ~p EVICTED\n\n", [NodeName]),
            orddict:erase(NodeName, AckMap);
        false ->
            orddict:store(NodeName, {LastAck, NextRound}, AckMap)
    end.

%% @private
eviction_round_number() ->
    ldb_config:get(ldb_eviction_round_number).

%% @private
schedule_dbuffer_shrink() ->
    Interval = 5000,
    %% tell the backend to try to shrink the dbuffer
    timer:send_after(Interval, dbuffer_shrink).
