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
         message_handler/4,
         message_size/1]).

-record(state, {id :: ldb_node_id(),
                node_number :: non_neg_integer(),
                gc :: boolean()}).
-type st() :: #state{}.

%% {crdt, map buffer, matrix}
-type stored() :: {term(), maps:map(dot(), term()), m_vclock()}.

-spec backend_state() -> st().
backend_state() ->
    Id = ldb_config:id(),
    NodeNumber = ldb_config:get(node_number),
    GC = ldb_config:get(ldb_scuttlebutt_gc, false),
    #state{id=Id,
           node_number=NodeNumber,
           gc=GC}.

-spec bottom_entry(term(), st()) -> stored().
bottom_entry(Bottom, #state{id=Id, node_number=NodeNumber}) ->
    DeltaBuffer = maps:new(),
    Matrix = m_vclock:new(Id, NodeNumber),
    {Bottom, DeltaBuffer, Matrix}.

-spec crdt(stored()) -> term().
crdt({CRDT, _, _}) ->
    CRDT.

-spec update(stored(), operation(), st()) -> stored().
update({{Type, _}=CRDT, DeltaBuffer, Matrix0}, Operation, #state{id=Id}) ->
    {ok, Delta} = Type:delta_mutate(Operation, Id, CRDT),
    {Dot, _, Matrix} = m_vclock:next_dot(Matrix0),
    store_delta(Dot, Delta, {CRDT, DeltaBuffer, Matrix}, true).

-spec memory(stored()) -> size_metric().
memory({CRDT, DeltaBuffer, Matrix}) ->
    {M, C} = message_size({dotted_buffer, DeltaBuffer}),
    Alg = M + C + ldb_util:size(matrix, m_vclock:matrix(Matrix)),
    {Alg, ldb_util:size(crdt, CRDT)}.

-spec message_maker(stored(), ldb_node_id(), st()) -> message().
message_maker({_CRDT, DeltaBuffer, Matrix}, _, #state{gc=GC}) ->
    case maps:size(DeltaBuffer) of
        0 ->
            nothing;
        _ ->
            case GC of
                true ->
                    {
                        matrix,
                        m_vclock:matrix(Matrix)
                    };
                false ->
                    {
                        vector,
                        m_vclock:get(Matrix)
                    }
            end
    end.

-spec message_handler(message(), ldb_node_id(), stored(), st()) ->
    {stored(), nothing | message()}.
message_handler({matrix, RemoteMatrix}, From,
                {CRDT, DeltaBuffer0, Matrix0}, _) ->

    %% prune what's stable
    Matrix1 = m_vclock:union_matrix(Matrix0, RemoteMatrix),

    %% get new stable dots
    {StableDots, Matrix2} = m_vclock:stable(Matrix1),

    %% prune these stable dots
    DeltaBuffer1 = maps:without(StableDots, DeltaBuffer0),
    Stored = {CRDT, DeltaBuffer1, Matrix2},

    %% extract remote vv
    RemoteVV = maps:get(From, RemoteMatrix),

    %% send buffer subset
    Reply = {
        dotted_buffer,
        find_dots(RemoteVV, DeltaBuffer1)
    },

    {Stored, Reply};

message_handler({vector, RemoteVV}, _From,
                {_, DeltaBuffer, _}=Stored, _) ->

    %% send buffer subset
    Reply = {
        dotted_buffer,
        find_dots(RemoteVV, DeltaBuffer)
    },
    {Stored, Reply};

message_handler({dotted_buffer, Buffer}, _From, Stored0, _) ->
    Stored = maps:fold(
        fun(Dot, Delta, StoreValueAcc) ->
            store_delta(Dot, Delta, StoreValueAcc, false)
        end,
        Stored0,
        Buffer
    ),
    {Stored, nothing}.

%% @private find dots that do not exist in the remote node.
-spec find_dots(vclock(), maps:map(dot(), term())) -> maps:map(dot(), term()).
find_dots(RemoteVV, DeltaBuffer) ->
    maps:filter(
        fun(Dot, _) -> not vclock:is_element(Dot, RemoteVV) end,
        DeltaBuffer
    ).

-spec message_size(message()) -> size_metric().
message_size({matrix, Matrix}) ->
    {ldb_util:size(matrix, Matrix), 0};
message_size({vector, Vector}) ->
    {ldb_util:size(vector, Vector), 0};
message_size({dotted_buffer, Buffer}) ->
    maps:fold(
        fun(_Dot, Delta, {M, C}) ->
            {M + 1, C + ldb_util:size(crdt, Delta)}
        end,
        {0, 0},
        Buffer
    ).

%% @private
-spec store_delta(dot(), term(), stored(), boolean()) -> stored().
store_delta(Dot, Delta, {{Type, _}=CRDT0, DeltaBuffer0, Matrix0}, AlreadyAdded) ->
    CRDT = Type:merge(Delta, CRDT0),
    DeltaBuffer = maps:put(Dot, Delta, DeltaBuffer0),
    Matrix = case AlreadyAdded of
        true -> Matrix0;
        false -> m_vclock:add_dot(Dot, Matrix0)
    end,
    {CRDT, DeltaBuffer, Matrix}.
