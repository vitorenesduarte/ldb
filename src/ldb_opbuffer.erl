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

-export([new/3,
         add_op/2,
         select/2,
         ack/3,
         size/1]).

-export_type([buffer/0]).


-record(buffer, {ii :: boolean(), %% use implicit info or not
                 id :: ldb_node_id(),
                 matrix :: m_vclock(),
                 buffer :: [entry()],
                 seen_by_map :: seen_by_map()}).
-record(entry, {op :: op(),
                dot :: dot(),
                vv :: vclock(),
                is_delivered :: boolean()}).
-type op() :: term().
-type seen_by_map() :: map:map(dot(), sets:set(ldb_node_id())).
-type entry() :: #entry{}.
-type buffer() :: #buffer{}.

%% aditional types
-type remote() :: {remote, op(), dot(), vclock(), ldb_node_id()}.
-type add_op_args() :: remote() | {local, op()}.
-type to_deliver() :: [{op(), vclock()}].


%% @doc Create new buffer.
-spec new(boolean(), ldb_node_id(), non_neg_integer()) -> buffer().
new(II, Id, NodeNumber) ->
    #buffer{ii=II,
            id=Id,
            matrix=m_vclock:new(Id, NodeNumber),
            buffer=[],
            seen_by_map=maps:new()}.

%% @doc Add a new operation to the buffer.
-spec add_op(add_op_args(), buffer()) -> {to_deliver(), buffer()}.
add_op({remote, Op, {Origin, _}=RemoteDot, RemoteVV, From}, #buffer{ii=II,
                                                                    matrix=Matrix0,
                                                                    buffer=Buffer0,
                                                                    seen_by_map=SeenByMap0}=State0) ->

    %% get current vv
    VV0 = m_vclock:get(Matrix0),

    %% create/update entry in the buffer
    {ShouldTry, SeenByMap2, Matrix2, Buffer2} = case maps:find(RemoteDot, SeenByMap0) of
        {ok, CurrentSeenBy} ->
            %% if exists, just update seen by map
            SeenByMap1 = case II of
                true -> maps:put(RemoteDot, sets:add_element(From, CurrentSeenBy), SeenByMap0);
                false -> SeenByMap0
            end,
            {false, SeenByMap1, Matrix0, Buffer0};

        error ->
            case vclock:is_element(RemoteDot, VV0) of
                true ->
                    %% not in map, and already delivered: do nothing
                    {false, SeenByMap0, Matrix0, Buffer0};
                false ->
                    %% otherwise, create op
                    Entry = #entry{op=Op,
                                   dot=RemoteDot,
                                   vv=RemoteVV,
                                   is_delivered=false},

                    %% compute seen by
                    SeenBy = case II of
                        true -> sets:from_list([Origin, From]);
                        false -> sets:from_list([From])
                    end,

                    %% update seen by map
                    SeenByMap1 = maps:put(RemoteDot, SeenBy, SeenByMap0),

                    %% update matrix and buffer
                    Matrix1 = m_vclock:update(Origin, RemoteVV, Matrix0),
                    Buffer1 = [Entry|Buffer0],
                    {true, SeenByMap1, Matrix1, Buffer1}
            end
    end,

    %% try to deliver, if it was a new op
    case ShouldTry of
        true ->
            %% try deliver
            {ToDeliver, VV, Buffer3} = try_deliver(Buffer2, [], VV0, []),

            %% update matrix and get stable dots
            Matrix3 = m_vclock:put(VV, Matrix2),
            {StableDots, Matrix} = m_vclock:stable(Matrix3),

            %% update buffer and seen by map
            Buffer = prune(Buffer3, sets:from_list(StableDots), []),
            SeenByMap = maps:without(StableDots, SeenByMap2),

            %% update state
            State = State0#buffer{buffer=Buffer,
                                  matrix=Matrix,
                                  seen_by_map=SeenByMap},
            {lists:reverse(ToDeliver), State};

        false ->
            %% just update state
            State = State0#buffer{buffer=Buffer2,
                                  matrix=Matrix2,
                                  seen_by_map=SeenByMap2},
            {[], State}
    end;

add_op({local, Op}, #buffer{matrix=Matrix0,
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
    SeenByMap = maps:put(Dot, sets:new(), SeenByMap0),

    %% update state
    State = State0#buffer{matrix=Matrix,
                          buffer=Buffer,
                          seen_by_map=SeenByMap},

    %% return to deliver, and updated state
    {[{Op, VV}], State}.

%% @doc Select which operations in the buffer should be sent to this neighbor.
-spec select(ldb_node_id(), buffer()) -> [{remote, op(), dot(), vclock(), ldb_node_id()}].
select(To, #buffer{id=Id,
                   buffer=Buffer,
                   seen_by_map=SeenByMap}) ->
    lists:filtermap(
        fun(#entry{op=Op, dot=Dot, vv=VV}) ->
            %% get nodes that saw this op (that we know of)
            SeenBy = maps:get(Dot, SeenByMap),

            %% if this neighbor didn't, send
            case not sets:is_element(To, SeenBy) of
                true -> {true, {remote, Op, Dot, VV, Id}};
                false -> false
            end
        end,
        Buffer
    ).

%% @doc Process list of acks.
-spec ack(ldb_node_id(), [dot()], buffer()) -> buffer().
ack(From, Dots, #buffer{seen_by_map=SeenByMap0}=State) ->
    SeenByMap = lists:foldl(
        fun(Dot, Acc) ->
            %% it's possible to receive an ack of a stable dot
            %% so this function is a bit more complex than desirable
            case maps:find(Dot, Acc) of
                {ok, SeenBy} ->
                    maps:put(Dot, sets:add_element(From, SeenBy), Acc);
                error ->
                    %% here we avoid creating an entry that would live forever
                    Acc
            end
        end,
        SeenByMap0,
        Dots
    ),
    State#buffer{seen_by_map=SeenByMap}.

%% @doc Compute size of the op-buffer.
-spec size(buffer()) -> non_neg_integer().
size(#buffer{matrix=Matrix,
             buffer=Buffer,
             seen_by_map=SeenByMap}) ->

    %% compute matrix size
    MatrixSize = ldb_util:size(matrix, m_vclock:matrix(Matrix)),

    %% compute buffer size
    BufferSize = lists:foldl(
        fun(#entry{op=Op, vv=VV}, Acc) ->
            %% +1 for the dot
            Acc + 1 + ldb_util:size(op, Op) + ldb_util:size(vector, VV)
        end,
        0,
        Buffer
    ),

    %% compute seen by map size
    SeenByMapSize = maps:fold(
        fun(_, SeenBy, Acc) ->
            Acc + sets:size(SeenBy) + 1
        end,
        0,
        SeenByMap
    ),

    MatrixSize + BufferSize + SeenByMapSize.

%% @private Try deliver operations.
-spec try_deliver([entry()], to_deliver(), vclock(), [entry()]) ->
    {to_deliver(), vclock(), [entry()]}.

try_deliver([#entry{is_delivered=false, op=Op, dot=RemoteDot, vv=RemoteVV}=Entry0|Rest],
            ToDeliver0, LocalVV0, Buffer0) ->
    %% if not delivered
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

try_deliver([Entry|Rest], ToDeliver, LocalVV, Buffer) ->
    %% if already delivered, just skip it
    %% and move entry to bufffer
    try_deliver(Rest, ToDeliver, LocalVV, [Entry|Buffer]);

try_deliver([], ToDeliver, LocalVV, Buffer) ->
    %% we're done: there's nothing else that can be delivered
    {ToDeliver, LocalVV, Buffer}.


%% @private Append to the right list the reverse of the left list.
%%          This is nice if we don't care about order since it avoids the use of ++.
-spec append_reverse(list(), list()) -> list().
append_reverse([H|T], L) -> append_reverse(T, [H|L]);
append_reverse([], L) -> L.

%% @private Prune dots from buffer.
-spec prune([entry()], sets:set(dot()), [entry()]) -> [entry()].
prune([#entry{dot=Dot}=Entry|Rest], Dots, Buffer0) ->
    Buffer = case sets:is_element(Dot, Dots) of
        true -> Buffer0;
        false -> [Entry|Buffer0]
    end,
    prune(Rest, Dots, Buffer);
prune([], _, Buffer) ->
    Buffer.

-ifdef(TEST).

add_local_op_test() ->
    Buffer0 =  new(true, a, 1),
    OpA = op_a,
    OpB = op_b,

    {[{OpA, VVA}], Buffer1} = add_op({local, OpA}, Buffer0),
    {[{OpB, VVB}], _} = add_op({local, OpB}, Buffer1),

    ?assertEqual(vclock:from_list([{a, 1}]), VVA),
    ?assertEqual(vclock:from_list([{a, 2}]), VVB).

add_remote_op_test() ->
    Buffer0 =  new(true, z, 3),

    OpA = op_a,
    DotA = {a, 1},
    VVA = vclock:from_list([{a, 1}, {b, 2}]),
    RemoteA = {remote, OpA, DotA, VVA, a},

    OpB = op_b,
    DotB = {b, 1},
    VVB = vclock:from_list([{b, 1}, {c, 1}]),
    RemoteB = {remote, OpB, DotB, VVB, b},

    OpC = op_c,
    DotC = {c, 1},
    VVC = vclock:from_list([{c, 1}]),
    RemoteC = {remote, OpC, DotC, VVC, c},

    OpD = op_d,
    DotD = {b, 2},
    VVD = vclock:from_list([{b, 2}, {c, 2}]),
    RemoteD = {remote, OpD, DotD, VVD, b},

    OpE = op_e,
    DotE = {c, 2},
    VVE = vclock:from_list([{c, 2}]),
    RemoteE = {remote, OpE, DotE, VVE, c},

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

select_ack_test() ->
    BufferA0 = new(true, a, 3),
    BufferB0 = new(true, b, 3),
    BufferC0 = new(true, c, 3),

    %% A does local op
    {_, BufferA1} = add_op({local, opa}, BufferA0),
    %% B has nothing to A and C
    [] = select(a, BufferB0),
    [] = select(c, BufferB0),
    %% A sends to B
    [RemoteAB1] = select(b, BufferA1),
    ?assertEqual({remote, opa, {a, 1}, vclock:from_list([{a, 1}]), a}, RemoteAB1),

    %% B receives
    {_, BufferB1} = add_op(RemoteAB1, BufferB0),
    %% B sends to C
    [] = select(a, BufferB1),
    [RemoteBC1]= select(c, BufferB1),
    ?assertEqual({remote, opa, {a, 1}, vclock:from_list([{a, 1}]), b}, RemoteBC1),

    %% C has nothing to A and B
    {_, BufferC1} = add_op(RemoteBC1, BufferC0),
    [] = select(a, BufferC1),
    [] = select(b, BufferC1),

    %% B acks A
    BufferA2 = ack(b, [{a, 1}], BufferA1),
    %% A has nothing to B
    [] = select(b, BufferA2),
    %% A sends to C
    [RemoteAC1] = select(c, BufferA2),
    ?assertEqual({remote, opa, {a, 1}, vclock:from_list([{a, 1}]), a}, RemoteAC1),

    %% A does local op
    {_, BufferA3} = add_op({local, opb}, BufferA2),
    %% C acks A
    BufferA4 = ack(c, [{a, 1}], BufferA3),
    %% A sends to C
    [RemoteAC2] = select(c, BufferA4),
    ?assertEqual({remote, opb, {a, 2}, vclock:from_list([{a, 2}]), a}, RemoteAC2),

    %% C receives both
    {_, BufferC2} = add_op(RemoteAC1, BufferC1),
    {_, BufferC3} = add_op(RemoteAC2, BufferC2),
    %% C has nothing to A
    [] = select(a, BufferC3),
    %% C does local op
    {_, BufferC4} = add_op({local, opc}, BufferC3),
    %% C sends to A
    [RemoteCA1] = select(a, BufferC4),
    ?assertEqual({remote, opc, {c, 1}, vclock:from_list([{a, 2}, {c, 1}]), c}, RemoteCA1),
    %% C sends to B
    [RemoteCB1, RemoteBC2] = select(b, BufferC4),
    ?assertEqual({remote, opc, {c, 1}, vclock:from_list([{a, 2}, {c, 1}]), c}, RemoteCB1),
    ?assertEqual({remote, opb, {a, 2}, vclock:from_list([{a, 2}]), c}, RemoteBC2).

prune_test() ->
    BufferA0 = new(true, a, 2),
    BufferB0 = new(true, b, 2),

    %% A does local op
    {_, BufferA1} = add_op({local, opa}, BufferA0),
    %% A sends to B
    [RemoteAB1] = select(b, BufferA1),
    %% B receives
    {_, BufferB1} = add_op(RemoteAB1, BufferB0),

    %% this op is stable on B, but not on A
    ?assertEqual(1, length(BufferA1#buffer.buffer)),
    ?assertEqual(0, length(BufferB1#buffer.buffer)),

    %% B does local op
    {_, BufferB2} = add_op({local, opb}, BufferB1),
    %% B sends to A
    [RemoteBA1] = select(a, BufferB2),
    %% A receives
    {_, BufferA2} = add_op(RemoteBA1, BufferA1),

    %% this op is stable on A, but not on B
    ?assertEqual(0, length(BufferA2#buffer.buffer)),
    ?assertEqual(1, length(BufferB2#buffer.buffer)).

%% @private
all_delivered(Buffer) ->
    lists:all(
        fun(#entry{is_delivered=IsDelivered}) -> IsDelivered end,
        Buffer#buffer.buffer
    ).
-endif.
