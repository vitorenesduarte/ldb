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
         from_list/1,
         get_next_dot/2,
         add_dot/2,
         is_element/2,
         is_inflation/2,
         can_deliver/3,
         union/2,
         intersection/2,
         subtract/2,
         size/1]).

-export_type([v/0]).

-type v() :: maps:map(ldb_node_id(), sequence()).

%% @doc Create an vclock.
-spec new() -> v().
new() ->
    maps:new().

%% @doc Create a vclock from a list of sequences.
-spec from_list([{ldb_node_id(), sequence()}]) -> v().
from_list(L) ->
    maps:from_list(L).

%% @doc Return the next dot for a given id.
-spec get_next_dot(ldb_node_id(), v()) -> dot().
get_next_dot(Id, Clock) ->
    Seq = maps:get(Id, Clock, 0),
    NewSeq = Seq + 1,
    {Id, NewSeq}.

%% @doc Add a dot to the vv.
-spec add_dot(dot(), v()) -> v().
add_dot({Id, Seq}, Clock) ->
    maps:update_with(
        Id,
        fun(CurrentSeq) -> max(Seq, CurrentSeq) end,
        Seq,
        Clock
    ).

%% @doc Check if a dot is in the clock.
-spec is_element(dot(), v()) -> boolean().
is_element({Id, Seq}, Clock) ->
    CurrentSeq = maps:get(Id, Clock, 0),
    Seq =< CurrentSeq.

%% @doc Check is a `ClockB' dominates `ClockA'.
-spec is_inflation(v(), v()) -> boolean().
is_inflation(ClockA, ClockB) ->
    is_inflation_loop(maps:to_list(ClockA), ClockB).

%% @private
-spec is_inflation_loop([{ldb_node_id(), non_neg_integer()}], v()) -> boolean().
is_inflation_loop([], _) ->
    true;
is_inflation_loop([Dot|Rest], ClockB) ->
    case is_element(Dot, ClockB) of
        true -> is_inflation_loop(Rest, ClockB);
        false -> false
    end.

%% @doc Check is a clock dominates another with the exception of the origin dot.
-spec can_deliver(dot(), v(), v()) -> boolean().
can_deliver({Id, Seq}=_RemoteDot, RemoteClock, LocalClock) ->
    case is_element({Id, Seq - 1}, LocalClock) of
        true -> is_inflation(maps:remove(Id, RemoteClock), LocalClock);
        false -> false
    end.

%% @doc Union clocks.
-spec union(v(), v()) ->v().
union(ClockA, ClockB) ->
    maps_ext:merge_all(
        fun(_, SeqA, SeqB) -> max(SeqA, SeqB) end,
        ClockA,
        ClockB
    ).

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

%% @doc Size of clock.
-spec size(v()) -> non_neg_integer().
size(Clock) ->
    maps:size(Clock).

-ifdef(TEST).

is_inflation_test() ->
    Bottom = #{},
    VClockA = #{a => 4, b => 1},
    VClockB = #{a => 6, c => 3},
    VClockC = #{a => 6, b => 1, c => 3},
    ?assert(is_inflation(Bottom, VClockA)),
    ?assert(is_inflation(Bottom, VClockB)),
    ?assert(is_inflation(Bottom, VClockC)),
    ?assert(is_inflation(VClockA, VClockA)),
    ?assertNot(is_inflation(VClockA, VClockB)),
    ?assertNot(is_inflation(VClockB, VClockA)),
    ?assert(is_inflation(VClockA, VClockC)),
    ?assert(is_inflation(VClockB, VClockC)),
    ?assertNot(is_inflation(VClockC, VClockA)),
    ?assertNot(is_inflation(VClockC, VClockB)).

can_deliver_test() ->
    VClockA = #{b => 1},
    VClockB = #{a => 6},
    VClockC = #{a => 4, c => 4},
    Local = #{a => 5, c => 3},
    ?assert(can_deliver({b, 1}, VClockA, Local)),
    ?assert(can_deliver({a, 6}, VClockB, Local)),
    ?assertNot(can_deliver({c, 5}, VClockB, Local)),
    ?assert(can_deliver({c, 4}, VClockC, Local)).

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
