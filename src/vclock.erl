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
