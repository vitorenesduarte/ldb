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

-export([new/1,
         add_op/2]).

-export_type([buffer/0]).


-record(buffer, {actor :: ldb_node_id(),
                 vv :: vclock(),
                 buffer :: [entry()],
                 seen_by_map :: seen_by_map()}).
-record(entry, {op :: op(),
                dot :: dot(),
                past :: vclock(),
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
-spec new(ldb_node_id()) -> buffer().
new(Actor) ->
    #buffer{actor=Actor,
            vv=vclock:new(),
            buffer=[],
            seen_by_map=maps:new()}.

%% @doc Add a new operation to the buffer.
-spec add_op(add_op_args(), buffer()) -> {to_deliver(), buffer()}.
add_op({local, Op}, #buffer{actor=Actor,
                            vv=VV0,
                            buffer=Buffer0,
                            seen_by_map=SeenByMap0}=State0) ->
    %% create identifier for this op
    Dot = vclock:next_dot(Actor, VV0),

    %% create op entry
    Entry = #entry{op=Op,
                   dot=Dot,
                   past=VV0,
                   is_delivered=true},

    %% update vv, buffer and seen by map
    VV = vclock:add_dot(Dot, VV0),
    Buffer = [Entry | Buffer0],
    SeenByMap = maps:put(Dot, sets:from_list([Actor]), SeenByMap0),

    %% update state
    State = State0#buffer{vv=VV,
                          buffer=Buffer,
                          seen_by_map=SeenByMap},

    %% return to deliver, and updated state
    {[{Op, VV}], State};

add_op({remote, Op, RemoteDot, RemoteVV, SeenBy0}, #buffer{actor=Actor,
                                                           buffer=Buffer0,
                                                           seen_by_map=SeenByMap0}=State0) ->

    %% update seen by
    SeenBy1 = sets:add_element(Actor, SeenBy0),

    %% create/update entry in the buffer
    {ShouldTry, SeenBy, Buffer} = case maps:find(RemoteDot, SeenByMap0) of
        {ok, CurrentSeenBy} ->
            %% if exists, just update seen by map
            {false, sets:union(CurrentSeenBy, SeenBy1), Buffer0};
        error ->
            %% otherwise, create op
            Entry = #entry{op=Op,
                           dot=RemoteDot,
                           past=RemoteVV,
                           is_delivered=false},
            {true, SeenBy1, [Entry|Buffer0]}
    end,
    
    %% update seen by map
    SeenByMap = maps:put(RemoteDot, SeenBy, SeenByMap0),

    %% update state
    State1 = State0#buffer{seen_by_map=SeenByMap},
    
    %% try to deliver, if it was a new op
    case ShouldTry of
        true ->
            State2 = State1#buffer{buffer=[]},
            {ToDeliver, State} = try_deliver(Buffer, [], State2),
            {lists:reverse(ToDeliver), State};

        false ->
            State = State1#buffer{buffer=Buffer},
            {[], State}
    end.

%% @private Try deliver operations.
-spec try_deliver([entry()], to_deliver(), buffer()) -> {to_deliver(), buffer()}.
try_deliver([#entry{is_delivered=true}=Entry|Rest],
            ToDeliver,
            #buffer{buffer=Buffer}=State0) ->
    %% if already delivered, just skip it
    %% and move entry to the bufffer
    State = State0#buffer{buffer=[Entry|Buffer]},
    try_deliver(Rest, ToDeliver, State);

try_deliver([#entry{op=Op, dot=RemoteDot, past=RemoteVV}=Entry0|Rest],
            ToDeliver0,
            #buffer{vv=LocalVV0, buffer=Buffer}=State0) ->
    case vclock:can_deliver(RemoteDot, RemoteVV, LocalVV0) of
        true ->
            %% if we can deliver
            %% update entry and vv
            Entry = Entry0#entry{is_delivered=true},
            LocalVV = vclock:add_dot(RemoteDot, LocalVV0),

            %% compute op final vv and update to deliver
            FinalVV = vclock:add_dot(RemoteDot, RemoteVV),
            ToDeliver = [{Op, FinalVV}|ToDeliver0],

            %% since we just delivered something,
            %% start again and try to deliver all pending entries
            AllEntries = append_reverse(Rest, [Entry|Buffer]),
            State = State0#buffer{vv=LocalVV, buffer=[]},
            try_deliver(AllEntries, ToDeliver, State);

        false ->
            %% if we can't, we just skip it
            State = State0#buffer{buffer=[Entry0|Buffer]},
            try_deliver(Rest, ToDeliver0, State)
    end;

try_deliver([], ToDeliver, State) ->
    %% we're done: there's nothing else that can be delivered
    {ToDeliver, State}.


%% @private Append to the right list the reverse of the left list.
%%          This is nice if we don't care about order since it avoids the use of ++.
-spec append_reverse(list(), list()) -> list().
append_reverse([], L) -> L;
append_reverse([H|T], L) -> append_reverse(T, [H|L]).

-ifdef(TEST).

add_local_op_test() ->
    Buffer0 =  new(a),
    OpA = op_a,
    OpB = op_b,

    {[{OpA, VVA}], Buffer1} = add_op({local, OpA}, Buffer0),
    {[{OpB, VVB}], _} = add_op({local, OpB}, Buffer1),

    ?assertEqual(vclock:from_list([{a, 1}]), VVA),
    ?assertEqual(vclock:from_list([{a, 2}]), VVB).

add_remote_op_test() ->
    Buffer0 =  new(a),
    SeenBy = sets:new(),

    OpA = op_a,
    DotA = {a, 1},
    VVA = vclock:from_list([{b, 2}]),

    OpB = op_b,
    DotB = {b, 1},
    VVB = vclock:from_list([{c, 1}]),
    
    OpC = op_c,
    DotC = {c, 1},
    VVC = vclock:from_list([]),

    OpD = op_d,
    DotD = {b, 2},
    VVD = vclock:from_list([{b, 1}, {c, 2}]),

    OpE = op_e,
    DotE = {c, 2},
    VVE = vclock:from_list([{c, 1}]),

    {ToDeliver1, Buffer1} = add_op({remote, OpA, DotA, VVA, SeenBy}, Buffer0),
    {ToDeliver2, Buffer2} = add_op({remote, OpB, DotB, VVB, SeenBy}, Buffer1),
    {ToDeliver3, Buffer3} = add_op({remote, OpC, DotC, VVC, SeenBy}, Buffer2),
    {ToDeliver4, Buffer4} = add_op({remote, OpD, DotD, VVD, SeenBy}, Buffer3),
    {ToDeliver5, _Buffer5} = add_op({remote, OpE, DotE, VVE, SeenBy}, Buffer4),

    ?assertEqual([], ToDeliver1),
    ?assertEqual([], ToDeliver2),
    ?assertEqual([{OpC, vclock:from_list([{c, 1}])},
                  {OpB, vclock:from_list([{b, 1}, {c, 1}])}], ToDeliver3),
    ?assertEqual([], ToDeliver4),
    ?assertEqual([{OpE, vclock:from_list([{c, 2}])},
                  {OpD, vclock:from_list([{b, 2}, {c, 2}])},
                  {OpA, vclock:from_list([{a, 1}, {b, 2}])}], ToDeliver5).
-endif.
