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
                node_number :: non_neg_integer()}).
-type st() :: #state{}.

%% {crdt, vv, map buffer, matrix}
-type stored() :: {term(), vclock(), maps:map(dot(), term()), m_vclock()}.

-spec backend_state() -> st().
backend_state() ->
    Actor = ldb_config:id(),
    NodeNumber = ldb_config:get(node_number),
    #state{actor=Actor,
           node_number=NodeNumber}.

-spec bottom_entry(term(), st()) -> stored().
bottom_entry(Bottom, #state{node_number=NodeNumber}) ->
    VV = vclock:new(),
    DeltaBuffer = maps:new(),
    Matrix = m_vclock:new(NodeNumber),
    {Bottom, VV, DeltaBuffer, Matrix}.

-spec crdt(stored()) -> term().
crdt({CRDT, _, _, _}) ->
    CRDT.

-spec update(stored(), operation(), st()) -> stored().
update({{Type, _}=CRDT0, VV0, _, _}=Stored, Operation, #state{actor=Actor}) ->
    {ok, Delta} = Type:delta_mutate(Operation, Actor, CRDT0),
    Dot = vclock:next_dot(Actor, VV0),
    store_delta(Actor, Dot, Delta, Stored).

-spec memory(stored()) -> two_size_metric().
memory({CRDT, _VV, DeltaBuffer, Matrix}) ->
    %% crdt
    C = ldb_util:size(crdt, CRDT),
    %% rest = delta buffer + matrix
    R = ldb_util:plus(
        ldb_util:size(dotted_buffer, DeltaBuffer),
        ldb_util:size(matrix, m_vclock:matrix(Matrix))
    ),
    {C, R}.

-spec message_maker(stored(), ldb_node_id(), st()) -> message().
message_maker({_CRDT, _VV, DeltaBuffer, Matrix}, _, _) ->
    case maps:size(DeltaBuffer) of
        0 ->
            nothing;
        _ ->
            {
                matrix,
                m_vclock:matrix(Matrix)
            }
    end.

-spec after_sync(stored()) -> stored().
after_sync(Stored) ->
    Stored.

-spec message_handler(message(), ldb_node_id(), stored(), st()) ->
    {stored(), nothing | message()}.
message_handler({matrix, RemoteMatrix}, From,
                {CRDT, VV, DeltaBuffer0, Matrix0}, _) ->

    %% prune what's stable
    Matrix1 = m_vclock:union_matrix(Matrix0, RemoteMatrix),

    %% get new stable dots
    {StableDots, Matrix2} = m_vclock:stable(Matrix1),

    %% prune these stable dots
    DeltaBuffer1 = maps:without(StableDots, DeltaBuffer0),
    Stored = {CRDT, VV, DeltaBuffer1, Matrix2},

    %% extract remote vv
    RemoteVV = maps:get(From, RemoteMatrix, vclock:new()),

    %% find dots that do not exist in the remote node
    Result = maps:filter(
        fun(Dot, _) -> not vclock:is_element(Dot, RemoteVV) end,
        DeltaBuffer1
    ),

    %% send buffer
    Reply = {
        dotted_buffer,
        Result
    },

    {Stored, Reply};

message_handler({dotted_buffer, Buffer}, _From,
                Stored0, #state{actor=Actor}) ->
    Stored = maps:fold(
        fun(Dot, Delta, StoreValueAcc) ->
            store_delta(Actor, Dot, Delta, StoreValueAcc)
        end,
        Stored0,
        Buffer
    ),
    {Stored, nothing}.

-spec message_size(message()) -> size_metric().
message_size({matrix, Matrix}) ->
    ldb_util:size(matrix, Matrix);
message_size({dotted_buffer, Buffer}) ->
    ldb_util:size(dotted_buffer, Buffer).

%% @private
-spec store_delta(ldb_node_id(), dot(), term(), stored()) -> stored().
store_delta(Actor, Dot, Delta, {{Type, _}=CRDT0, VV0, DeltaBuffer0, Matrix0}) ->
    CRDT1 = Type:merge(Delta, CRDT0),
    VV1 = vclock:add_dot(Dot, VV0),
    DeltaBuffer1 = maps:put(Dot, Delta, DeltaBuffer0),
    Matrix1 = m_vclock:update(Actor, VV1, Matrix0),
    {CRDT1, VV1, DeltaBuffer1, Matrix1}.
