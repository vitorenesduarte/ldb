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

%% @doc A delta-buffer that only keeps the maximal
%%      of each chain irreducibles.

-module(ldb_maximals_dbuffer).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-behaviour(ldb_dbuffer).

-include("ldb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/3,
         seq/1,
         min_seq/1,
         is_empty/1,
         add_inflation/3,
         select/3,
         prune/2]).

-export_type([d/0]).

-record(dbuffer, {avoid_bp :: boolean(),
                  to_kv_fun :: function(),
                  from_kv_fun :: function(),
                  min_seq :: sequence(),
                  seq :: sequence(),
                  buffer :: maps:map(term(), d_entry())}).
-type d() :: #dbuffer{}.

-record(dbuffer_entry, {from :: ldb_node_id(),
                        seq :: sequence(),
                        value :: term()}).
-type d_entry() :: #dbuffer_entry{}.

%% @doc Create new buffer.
-spec new(boolean(), function(), function()) -> d().
new(AvoidBP, ToKVFun, FromKVFun) ->
    #dbuffer{avoid_bp=AvoidBP,
             to_kv_fun=ToKVFun,
             from_kv_fun=FromKVFun,
             min_seq=0,
             seq=0,
             buffer=maps:new()}.

%% @doc Retrieve seq.
-spec seq(d()) -> sequence().
seq(#dbuffer{seq=Seq}) ->
    Seq.

%% @doc Retrieve min_seq.
-spec min_seq(d()) -> sequence().
min_seq(#dbuffer{min_seq=MinSeq}) ->
    MinSeq.

%% @doc Check if buffer is empty.
-spec is_empty(d()) -> boolean().
is_empty(#dbuffer{buffer=Buffer}) ->
    maps:size(Buffer) == 0.

%% @doc Add irreducible to buffer.
-spec add_inflation(term(), ldb_node_id(), d()) -> d().
add_inflation(Irreducible, From, #dbuffer{to_kv_fun=ToKVFun,
                                          seq=Seq0,
                                          buffer=Buffer0}=State) ->
    %% get kv
    {Key, Value} = ToKVFun(Irreducible),

    %% create entry
    Entry = #dbuffer_entry{from=From,
                           seq=Seq0,
                           value=Value},

    %% update buffer:
    %% - if indexing key exists, replace value
    %% - otherwise, store it
    Buffer = maps:update_with(
        Key,
        fun(_) -> Entry end,
        Entry,
        Buffer0
    ),

    %% update seq
    Seq = Seq0 + 1,

    %% update state
    State#dbuffer{seq=Seq, buffer=Buffer}.

%% @doc Select irreducibles from buffer.
-spec select(ldb_node_id(), sequence(), d()) -> [term()].
select(To, LastAck, #dbuffer{avoid_bp=AvoidBP,
                             from_kv_fun=FromKVFun,
                             buffer=Buffer}) ->
    maps:fold(
        fun(Key, #dbuffer_entry{from=From,
                                seq=Seq,
                                value=Value}, Acc) ->

            case ldb_dbuffer:should_send(From, Seq, To, LastAck, AvoidBP) of
                true ->
                    Irreducible = FromKVFun(Key, Value),
                    [Irreducible | Acc];
                false ->
                    Acc
            end
        end,
        [],
        Buffer
    ).

%% @doc Prune from buffer.
-spec prune(sequence(), d()) -> d().
prune(AllAck, #dbuffer{buffer=Buffer0}=State) ->
    Buffer = maps:filter(
        fun(_, #dbuffer_entry{seq=Seq}) -> Seq >= AllAck end,
        Buffer0
    ),
    State#dbuffer{min_seq=AllAck, buffer=Buffer}.

-ifdef(TEST).

dbuffer_gset_test() ->
    AvoidBP = true,
    ToKVFun = fun(E) -> {E, <<>>} end,
    FromKVFun = fun(E, <<>>) -> E end,
    Buffer0 =  new(AvoidBP, ToKVFun, FromKVFun),

    Buffer1 = add_inflation(elem1, a, Buffer0),
    ToA0 = select(a, 0, Buffer1),
    ToA1 = select(a, 0, Buffer1#dbuffer{avoid_bp=false}),
    ToA2 = select(a, 1, Buffer1),

    Buffer2 = add_inflation(elem2, b, Buffer1),
    ToA3 = select(a, 1, Buffer2),
    ToB0 = select(b, 0, Buffer2),

    Buffer3 = prune(1, Buffer2),
    ToA4 = select(a, 1, Buffer3),
    ToA5 = select(a, 2, Buffer3),

    Buffer4 = prune(2, Buffer3),
    %% given that we pruned 2, select 1 shouldn't occur, but:
    ToA6 = select(a, 1, Buffer4),

    Buffer5 = add_inflation(elem3, c, Buffer4),
    ToA7 = select(a, 2, Buffer5),
    ToB1 = select(b, 2, Buffer5),
    ToC0 = select(c, 2, Buffer5),

    ?assertEqual([], ToA0),
    ?assertEqual([elem1], ToA1),
    ?assertEqual([], ToA2),
    ?assertEqual([elem2], ToA3),
    ?assertEqual([elem1], ToB0),
    ?assertEqual([elem1], ToB0),
    ?assertEqual([elem2], ToA4),
    ?assertEqual([], ToA5),
    ?assertEqual([], ToA6),
    ?assertEqual([elem3], ToA7),
    ?assertEqual([elem3], ToB1),
    ?assertEqual([], ToC0),
    ok.

dbuffer_gcounter_test() ->
    AvoidBP = true,
    ToKVFun = fun({K, V}) -> {K, V} end,
    FromKVFun = fun(K, V) -> {K, V} end,
    Buffer0 =  new(AvoidBP, ToKVFun, FromKVFun),

    Buffer1 = add_inflation({k, v1}, a, Buffer0),
    ToB0 = select(b, 0, Buffer1),

    Buffer2 = add_inflation({k, v2}, a, Buffer1),
    ToB1 = select(b, 0, Buffer2),

    Buffer3 = prune(1, Buffer2),
    ToB2 = select(b, 1, Buffer3),

    ?assertEqual([{k, v1}], ToB0),
    ?assertEqual([{k, v2}], ToB1),
    ?assertEqual([{k, v2}], ToB2),
    ok.

-endif.
