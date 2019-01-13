%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
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

%% @doc A buffer of operations to be delivered / forward.

-module(ldb_opbuffer).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/2,
         add_op/2,
         select/2]).

-export_type([buffer/0]).


-record(buffer, {id :: ldb_node_id(),
                 matrix :: m_vclock(),
                 buffer :: [entry()],
                 seen_by_map :: seen_by_map()}).
-record(entry, {op :: op(),
                dot :: dot(),
                vv :: vclock(),
                is_delivered :: boolean()}).
-type op() :: term().
-type seen_by() :: sets:set(ldb_node_id()).
-type seen_by_map() :: map:map(dot(), seen_by()).
-type entry() :: #entry{}.
-type buffer() :: #buffer{}.

%% aditional types
-type add_op_args() :: {local, op()} |
                       {remote, op(), dot(), vclock(), seen_by()}.
-type to_deliver() :: [{op(), vclock()}].


%% @doc Create new buffer.
-spec new(ldb_node_id(), non_neg_integer()) -> buffer().
new(Id, NodeNumber) ->
    #buffer{id=Id,
            matrix=m_vclock:new(Id, NodeNumber),
            buffer=[],
            seen_by_map=maps:new()}.

%% @doc Add a new operation to the buffer.
-spec add_op(add_op_args(), buffer()) -> {to_deliver(), buffer()}.
add_op({local, Op}, #buffer{id=Id,
                            matrix=Matrix0,
                            buffer=Buffer0,
                            seen_by_map=SeenByMap0}=State0) ->
    %% create identifier for this op
    {Dot, VV, Matrix} = m_vclock:next_dot(Matrix0),

    %% create op entry
    Entry = #entry{op=Op,
                   dot=Dot,
                   vv=VV,
                   is_delivered=true},

    %% update buffer and seen by map
    Buffer = [Entry | Buffer0],
    SeenBy = sets:from_list([Id]),
    SeenByMap = maps:put(Dot, SeenBy, SeenByMap0),

    %% update state
    State = State0#buffer{matrix=Matrix,
                          buffer=Buffer,
                          seen_by_map=SeenByMap},

    %% return to deliver, and updated state
    {[{Op, VV}], State};

add_op({remote, Op, {From, _}=RemoteDot, RemoteVV, SeenBy0}, #buffer{id=Id,
                                                                     matrix=Matrix0,
                                                                     buffer=Buffer0,
                                                                     seen_by_map=SeenByMap0}=State0) ->

    %% update seen by
    SeenBy1 = sets:add_element(Id, SeenBy0),

    %% create/update entry in the buffer
    {ShouldTry, SeenBy, Matrix2, Buffer2} = case maps:find(RemoteDot, SeenByMap0) of
        {ok, CurrentSeenBy} ->
            %% if exists, just update seen by map
            SeenBy2 = sets:union(CurrentSeenBy, SeenBy1),
            {false, SeenBy2, Matrix0, Buffer0};

        error ->
            %% otherwise, create op
            Entry = #entry{op=Op,
                           dot=RemoteDot,
                           vv=RemoteVV,
                           is_delivered=false},

            %% update matrix and buffer
            Matrix1 = m_vclock:update(From, RemoteVV, Matrix0),
            Buffer1 = [Entry|Buffer0],
            {true, SeenBy1, Matrix1, Buffer1}
    end,

    %% update seen by map
    SeenByMap1 = maps:put(RemoteDot, SeenBy, SeenByMap0),

    %% try to deliver, if it was a new op
    case ShouldTry of
        true ->
            %% get current vv
            VV0 = m_vclock:get(Matrix2),

            %% try deliver
            {ToDeliver, VV, Buffer3} = try_deliver(Buffer2, [], VV0, []),

            %% update matrix and get stable dots
            Matrix3 = m_vclock:put(VV, Matrix2),
            {StableDots, Matrix} = m_vclock:stable(Matrix3),

            %% update buffer and seen by map
            Buffer = prune(sets:from_list(StableDots), Buffer3, []),
            SeenByMap = maps:without(StableDots, SeenByMap1),

            %% update state
            State = State0#buffer{buffer=Buffer,
                                  matrix=Matrix,
                                  seen_by_map=SeenByMap},
            {lists:reverse(ToDeliver), State};

        false ->
            %% just update state
            State = State0#buffer{buffer=Buffer2,
                                  matrix=Matrix2,
                                  seen_by_map=SeenByMap1},
            {[], State}
    end.

%% @doc Select which operations in the buffer should be sent to this neighbor.
-spec select(ldb_node_id(), buffer()) -> [{op(), dot(), vclock(), seen_by()}].
select(To, #buffer{buffer=Buffer,
                   seen_by_map=SeenByMap}) ->
    lists:filtermap(
        fun(#entry{op=Op, dot=Dot, vv=VV}) ->
            SeenBy = maps:get(Dot, SeenByMap),
            Include = not sets:is_element(To, SeenBy),
            {Include, {Op, Dot, VV, SeenBy}}
        end,
        Buffer
    ).

%% @doc Process list of acks.
%%      - if all neighbors have seen it, drop it from the buffer
%%      TODO here we're assuming static (partial) memberships.
%%           in general we should only prune when an op. is stable
%% -spec ack([dot()], buffer()) -> buffer().
%% ack(Dot,

%% @private Try deliver operations.
-spec try_deliver([entry()], to_deliver(), vclock(), [entry()]) -> {to_deliver(), vclock(), [entry()]}.
try_deliver([#entry{is_delivered=true}=Entry|Rest],
            ToDeliver, LocalVV, Buffer) ->
    %% if already delivered, just skip it
    %% and move entry to bufffer
    try_deliver(Rest, ToDeliver, LocalVV, [Entry|Buffer]);

try_deliver([#entry{op=Op, dot=RemoteDot, vv=RemoteVV}=Entry0|Rest],
            ToDeliver0, LocalVV0, Buffer0) ->
    case vclock:can_deliver(RemoteDot, RemoteVV, LocalVV0) of
        true ->
            %% if we can deliver
            %% update entry and vv
            Entry = Entry0#entry{is_delivered=true},
            LocalVV = vclock:add_dot(RemoteDot, LocalVV0),

            %% update to deliver
            ToDeliver = [{Op, RemoteVV}|ToDeliver0],

            %% since we just delivered something,
            %% start again and try to deliver all pending entries
            Buffer = append_reverse(Rest, [Entry|Buffer0]),
            try_deliver(Buffer, ToDeliver, LocalVV, []);

        false ->
            %% if we can't, we just skip it
            try_deliver(Rest, ToDeliver0, LocalVV0, [Entry0|Buffer0])
    end;

try_deliver([], ToDeliver, LocalVV, Buffer) ->
    %% we're done: there's nothing else that can be delivered
    {ToDeliver, LocalVV, Buffer}.


%% @private Append to the right list the reverse of the left list.
%%          This is nice if we don't care about order since it avoids the use of ++.
-spec append_reverse(list(), list()) -> list().
append_reverse([], L) -> L;
append_reverse([H|T], L) -> append_reverse(T, [H|L]).

%% @private Prune dots from buffer.
-spec prune(sets:set(dot()), [entry()], [entry()]) -> [entry()].
prune(Dots, [#entry{dot=Dot}=Entry|Rest], Buffer0) ->
    Buffer = case sets:is_element(Dot, Dots) of
        true -> Buffer0;
        false -> [Entry|Buffer0]
    end,
    prune(Dots, Rest, Buffer);
prune(_, [], Buffer) ->
    Buffer.

-ifdef(TEST).

add_local_op_test() ->
    Buffer0 =  new(a, 1),
    OpA = op_a,
    OpB = op_b,

    {[{OpA, VVA}], Buffer1} = add_op({local, OpA}, Buffer0),
    {[{OpB, VVB}], _} = add_op({local, OpB}, Buffer1),

    ?assertEqual(vclock:from_list([{a, 1}]), VVA),
    ?assertEqual(vclock:from_list([{a, 2}]), VVB).

add_remote_op_test() ->
    Buffer0 =  new(z, 3),
    SeenBy = sets:new(),

    OpA = op_a,
    DotA = {a, 1},
    VVA = vclock:from_list([{a, 1}, {b, 2}]),
    RemoteA = {remote, OpA, DotA, VVA, SeenBy},

    OpB = op_b,
    DotB = {b, 1},
    VVB = vclock:from_list([{b, 1}, {c, 1}]),
    RemoteB = {remote, OpB, DotB, VVB, SeenBy},

    OpC = op_c,
    DotC = {c, 1},
    VVC = vclock:from_list([{c, 1}]),
    RemoteC = {remote, OpC, DotC, VVC, SeenBy},

    OpD = op_d,
    DotD = {b, 2},
    VVD = vclock:from_list([{b, 2}, {c, 2}]),
    RemoteD = {remote, OpD, DotD, VVD, SeenBy},

    OpE = op_e,
    DotE = {c, 2},
    VVE = vclock:from_list([{c, 2}]),
    RemoteE = {remote, OpE, DotE, VVE, SeenBy},

    {ToDeliver1, Buffer1} = add_op(RemoteA, Buffer0),
    ?assertEqual([], ToDeliver1),

    {ToDeliver2, Buffer2} = add_op(RemoteB, Buffer1),
    ?assertEqual([], ToDeliver2),

    {ToDeliver3, Buffer3} = add_op(RemoteC, Buffer2),
    ?assertEqual([{OpC, VVC},
                  {OpB, VVB}], ToDeliver3),

    {ToDeliver4, Buffer4} = add_op(RemoteD, Buffer3),
    ?assertEqual([], ToDeliver4),

    {ToDeliver5, Buffer5} = add_op(RemoteE, Buffer4),
    ?assertEqual([{OpE, VVE},
                  {OpD, VVD},
                  {OpA, VVA}], ToDeliver5),

    %% check wether all ops are marked as delivered
    ?assertNot(all_delivered(Buffer4)),
    ?assert(all_delivered(Buffer5)),

    %% check we don't deliver twice
    {ToDeliver6, _} = add_op(RemoteE, Buffer5),
    ?assertEqual([], ToDeliver6).


%% @private
all_delivered(Buffer) ->
    lists:all(
        fun(#entry{is_delivered=IsDelivered}) -> IsDelivered end,
        Buffer#buffer.buffer
    ).
-endif.
