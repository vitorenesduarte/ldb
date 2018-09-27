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

%% @doc VClock.

-module(vclock).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/0,
         next_dot/2,
         is_element/2,
         union/2,
         intersection/2,
         subtract/2]).

-export_type([v/0]).

-type v() :: maps:map(ldb_node_id(), non_neg_integer()).

%% @doc Create an vclock.
-spec new() -> v().
new() ->
    maps:new().

%% @doc Return the next dot for a given id.
-spec next_dot(ldb_node_id(), v()) -> {dot(), vv()}.
next_dot(Id, Clock) ->
    Seq = maps:get(Id, Clock, 0),
    NewSeq = Seq + 1,
    Dot = {Id, NewSeq},
    {Dot, maps:put(Id, NewSeq, Clock)}.

%% @doc Check if a dot is in the clock.
-spec is_element(dot(), v()) -> boolean().
is_element({Id, Seq}, Clock) ->
    CurrentSeq = maps:get(Id, Clock, 0),
    Seq =< CurrentSeq.

%% @doc Union clocks.
-spec union(v(), v()) ->v().
union(ClockA, ClockB) ->
    %% merge A with B
    %% (what's in B that's not in A won't be in `Clock0')
    Clock0 = maps:fold(
        fun(Id, SeqA, Acc) ->
            SeqB = maps:get(Id, ClockB, 0),
            Seq = max(SeqA, SeqB),
            maps:put(Id, Seq, Acc)
        end,
        maps:new(),
        ClockA
    ),
    %% merge B with `Clock0'
    maps:merge(ClockB, Clock0).

%% @doc Intersect clocks.
-spec intersection(v(), v()) -> v().
intersection(ClockA, ClockB) ->
    Clock0 = maps:filter(
        fun(Id, _) -> maps:is_key(Id, ClockB) end,
        ClockA
    ),
    maps:map(
        fun(Id, SeqA) ->
            SeqB = maps:get(Id, ClockB),
            min(SeqA, SeqB)
        end,
        Clock0
    ).

%% @doc Subtract clocks.
-spec subtract(v(), v()) -> list(dot()).
subtract(ClockA, ClockB) ->
    maps:fold(
        fun(Id, SeqA, Acc0) ->
            SeqB = maps:get(Id, ClockB, 0),
            case SeqB >= SeqA of
                true ->
                    Acc0;
                false ->
                    lists:foldl(
                        fun(Seq, Acc1) -> [{Id, Seq} | Acc1] end,
                        Acc0,
                        lists:seq(SeqB + 1, SeqA)
                    )
            end
        end,
        [],
        ClockA
    ).


-ifdef(TEST).

union_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    Expected = #{a => 6, b => 1, c => 3},

    ?assertEqual(VClockA, union(Bottom, VClockA)),
    ?assertEqual(VClockA, union(VClockA, Bottom)),
    ?assertEqual(Expected, union(VClockA, VClockB)),
    ?assertEqual(Expected, union(VClockB, VClockA)),
    ok.

intersection_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    Expected = #{a => 4},

    ?assertEqual(Bottom, intersection(Bottom, VClockA)),
    ?assertEqual(Bottom, intersection(VClockA, Bottom)),
    ?assertEqual(Expected, intersection(VClockA, VClockB)),
    ?assertEqual(Expected, intersection(VClockB, VClockA)),
    ok.

subtract_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},

    ?assertEqual([], subtract(Bottom, VClockA)),
    ?assertEqual([{a, 1}, {a, 2}, {a, 3}, {a, 4}, {b, 1}], lists:sort(subtract(VClockA, Bottom))),
    ?assertEqual([{b, 1}], subtract(VClockA, VClockB)),
    ?assertEqual([{a, 5}, {a, 6}, {c, 1}, {c, 2}, {c, 3}], lists:sort(subtract(VClockB, VClockA))),
    ok.

-endif.
