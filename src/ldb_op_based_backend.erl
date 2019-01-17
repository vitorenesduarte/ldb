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

-module(ldb_op_based_backend).
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

-record(state, {ii :: boolean(),
                id :: ldb_node_id(),
                node_number :: non_neg_integer()}).
-type st() :: #state{}.

%% {crdt, buffer}
-type stored() :: {term(), opbuffer()}.

-spec backend_state() -> st().
backend_state() ->
    II = ldb_config:get(ldb_op_ii),
    Id = ldb_config:id(),
    NodeNumber = ldb_config:get(node_number),
    #state{ii=II,
           id=Id,
           node_number=NodeNumber}.

-spec bottom_entry(term(), st()) -> stored().
bottom_entry(Bottom, #state{ii=II, id=Id, node_number=NodeNumber}) ->
    Buffer = ldb_opbuffer:new(II, Id, NodeNumber),
    {Bottom, Buffer}.

-spec crdt(stored()) -> term().
crdt({CRDT, _}) ->
    CRDT.

-spec update(stored(), operation(), st()) -> stored().
update({{Type, _}=CRDT0, Buffer0}, Op, _) ->
    %% add to op buffer
    {[{Op, VV}], Buffer} = ldb_opbuffer:add_op({local, Op}, Buffer0),

    %% apply it to CRDT
    {ok, CRDT} = Type:mutate(Op, VV, CRDT0),

    %% return updated CRDT and buffer
    {CRDT, Buffer}.

-spec memory(stored()) -> size_metric().
memory({CRDT, Buffer}) ->
    Alg = ldb_opbuffer:size(Buffer),
    {Alg, ldb_util:size(crdt, CRDT)}.

-spec message_maker(stored(), ldb_node_id(), st()) -> message().
message_maker({_CRDT, Buffer}, To, _) ->
    case ldb_opbuffer:select(To, Buffer) of
        [] -> nothing;
        Ops -> {ops, Ops}
    end.

-spec message_handler(message(), ldb_node_id(), stored(), st()) ->
    {stored(), nothing | message()}.
message_handler({ops, Ops}, _From, {{Type, _}=CRDT0, Buffer0}, _) ->
    %% lager:info("OPS BY ~p: ~p", [ldb_util:show(id, From),
    %%                              ldb_util:show(ops, Ops)]),

    {CRDT, Buffer, Dots} = lists:foldl(
        fun({_, _, Dot, _, _}=Remote, {CRDTAcc0, BufferAcc0, DotsAcc0}) ->
            %% add dot to list of dots to ack
            DotsAcc = [Dot | DotsAcc0],

            %% add to buffer
            {ToDeliver, BufferAcc} = ldb_opbuffer:add_op(Remote, BufferAcc0),

            %% lager:info("DELIVER: ~p", [[ldb_util:show(vector, VV) || {_, VV} <- ToDeliver]]),

            %% apply all delivered ops to CRDT
            CRDTAcc = lists:foldl(
                fun({Op, VV}, CRDTAcc1) ->
                    {ok, CRDTAcc2} = Type:mutate(Op, VV, CRDTAcc1),
                    CRDTAcc2
                end,
                CRDTAcc0,
                ToDeliver
            ),

            {CRDTAcc, BufferAcc, DotsAcc}
        end,
        {CRDT0, Buffer0, []},
        Ops
    ),
    Stored = {CRDT, Buffer},

    %% send ack
    Reply = {ops_ack, Dots},

    {Stored, Reply};

message_handler({ops_ack, Dots}, From, {CRDT, Buffer0}, _) ->
    %% lager:info("ACK BY ~p: ~p", [ldb_util:show(id, From),
    %%                              ldb_util:show(dots, Dots)]),

    %% process ack
    Buffer = ldb_opbuffer:ack(From, Dots, Buffer0),
    Stored = {CRDT, Buffer},
    {Stored, nothing}.

-spec message_size(message()) -> size_metric().
message_size({ops, Ops}) ->
    lists:foldl(
        fun({_, Op, _Dot, VV, _}, {M, C}) ->
            {M + 1 + ldb_util:size(vector, VV), %% + 1 (Dot)
             C + ldb_util:size(op, Op)}
        end,
        {0, 0},
        Ops
    );
message_size({ops_ack, Dots}) ->
    {length(Dots), 0}.
