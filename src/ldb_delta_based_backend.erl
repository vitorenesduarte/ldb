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
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).

%% ldb_backend callbacks
-export([backend_state/0,
         bottom_entry/2,
         crdt/1,
         update/3,
         memory/1,
         message_maker/3,
         after_sync/1,
         message_handler/4,
         message_size/1]).

-record(state, {actor :: ldb_node_id(),
                bp :: boolean(),
                rr :: boolean()}).
-type st() :: #state{}.

%% {crdt, changed, buffer, ack map}
-type stored() :: {term(), boolean(), dbuffer(), maps:map(ldb_node_id(), non_neg_integer())}.

-spec backend_state() -> st().
backend_state() ->
    Actor = ldb_config:id(),
    BP = ldb_config:get(ldb_dgroup_back_propagation, false),
    RR = ldb_config:get(ldb_redundant_dgroups, false),
    #state{actor=Actor,
           bp=BP,
           rr=RR}.

-spec bottom_entry(term(), st()) -> stored().
bottom_entry(Bottom, #state{bp=BP}) ->
    %% create buffer
    DeltaBuffer = ldb_dbuffer:new(BP),
    %% create ack map
    AckMap = maps:new(),
    {Bottom, false, DeltaBuffer, AckMap}.

-spec crdt(stored()) -> term().
crdt({CRDT, _, _, _}) ->
    CRDT.

-spec update(stored(), operation(), st()) -> stored().
update({{Type, _}=CRDT0, _, DeltaBuffer0, AckMap}, Operation, #state{actor=Actor}) ->
    %% create delta
    {ok, Delta} = Type:delta_mutate(Operation, Actor, CRDT0),
    %% merge it
    CRDT = Type:merge(Delta, CRDT0),
    %% and add it to the buffer
    DeltaBuffer = ldb_dbuffer:add_inflation(Delta, Actor, DeltaBuffer0),
    {CRDT, true, DeltaBuffer, AckMap}.

-spec memory(stored()) -> two_size_metric().
memory({CRDT, _, DeltaBuffer, AckMap}) ->
    %% crdt
    C = ldb_util:size(crdt, CRDT),
    %% rest = delta buffer + ack map
    R = ldb_util:plus(
        ldb_dbuffer:size(DeltaBuffer),
        ldb_util:size(ack_map, AckMap)
    ),
    {C, R}.

-spec message_maker(stored(), ldb_node_id(), st()) -> message().
message_maker({CRDT, _Changed, DeltaBuffer, AckMap}, NodeName, _) ->
    %% case Changed of
    %%     true ->
            %% get seq and last ack
            Seq = ldb_dbuffer:seq(DeltaBuffer),
            LastAck = last_ack(NodeName, AckMap),

            case LastAck < Seq of
                true ->

                    %% there's something missing in `NodeName'
                    %% get min seq
                    MinSeq = ldb_dbuffer:min_seq(DeltaBuffer),
                    case ldb_dbuffer:is_empty(DeltaBuffer) orelse MinSeq > LastAck of
                        true ->
                            {
                                delta,
                                Seq,
                                CRDT
                            };
                        false ->

                            %% should send deltas
                            Delta = ldb_dbuffer:select(NodeName, LastAck, DeltaBuffer),

                            case Delta of
                                undefined ->
                                    nothing;
                                _ ->
                                    {
                                        delta,
                                        Seq,
                                        Delta
                                    }
                            end
                    end;

                false ->
                    nothing
        %%     end;
        %% false ->
        %%     nothing
    end.

-spec after_sync(stored()) -> stored().
after_sync({LocalCRDT, _, DeltaBuffer, AckMap}) ->
    {LocalCRDT, false, DeltaBuffer, AckMap}.

-spec message_handler(message(), ldb_node_id(), stored(), st()) ->
    {stored(), nothing | message()}.
message_handler({delta, N, {Type, _}=Remote}, From,
                {LocalCRDT0, Changed0, DeltaBuffer0, AckMap}, #state{rr=RR}) ->

    %% compute delta and merge
    {Delta, LocalCRDT} = Type:delta_and_merge(Remote, LocalCRDT0),

    %% add to buffer
    {Changed, DeltaBuffer} = case Type:is_bottom(Delta) of
        true ->
            %% no inflation
            {Changed0, DeltaBuffer0};
        false ->
            %% if inflation, add \Delta if RR, remote CRDT otherwise
            DeltaBuffer1 = case RR of
                true -> ldb_dbuffer:add_inflation(Delta, From, DeltaBuffer0);
                false -> ldb_dbuffer:add_inflation(Remote, From, DeltaBuffer0)
            end,
            {true, DeltaBuffer1}
    end,
    Stored = {LocalCRDT, Changed, DeltaBuffer, AckMap},

    %% send ack
    Reply = {
        delta_ack,
        N
    },

    {Stored, Reply};

message_handler({delta_ack, N}, From,
                {LocalCRDT, Changed, DeltaBuffer0, AckMap0}, _) ->
    %% when a new ack is received,
    %% update the number of rounds without
    %% receiving an ack to 0
    AckMap1 = maps:update_with(
        From,
        fun(LastAck) -> max(LastAck, N) end,
        N,
        AckMap0
    ),

    %% and try to shrink the delta-buffer immediately
    DeltaBuffer1 = ldb_dbuffer:prune(min_seq_ack_map(AckMap1), DeltaBuffer0),

    Stored = {LocalCRDT, Changed, DeltaBuffer1, AckMap1},
    {Stored, nothing}.

-spec message_size(message()) -> size_metric().
message_size({delta, _N, Delta}) ->
    %% ignore sequence N
    ldb_util:size(crdt, Delta);
message_size({delta_ack, _N}) ->
    %% ignore sequence N
    {0, 0}.

%% @private
min_seq_ack_map(AckMap) ->
    lists:min(maps:values(AckMap)).

%% @private
last_ack(NodeName, AckMap) ->
    maps:get(NodeName, AckMap, 0).
