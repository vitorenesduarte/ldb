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

%% @doc VClock "Matrix".

-module(m_vclock).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         update/3,
         stable/1]).

-export_type([m/0]).

-record(state, {node_number :: non_neg_integer(),
                stable :: vclock(),
                matrix :: maps:map(ldb_node_id(), vclock())}).
-type m() :: #state{}.


%% @doc Create an empty matrix.
-spec new(non_neg_integer()) -> m().
new(NodeNumber) ->
    #state{node_number=NodeNumber,
           stable=vclock:new(),
           matrix=maps:new()}.

%% @doc Update clock for a given sender.
-spec update(ldb_node_id(), vclock(), m()) -> m().
update(Id, Clock, #state{matrix=Matrix0}=State) ->
    Matrix = maps:update_with(
        Id,
        fun(Current) ->
            %% take the highest clock
            vclock:union(Clock, Current)
        end,
        Clock,
        Matrix0
    ),
    State#state{matrix=Matrix}.

%% @doc Get list of stable dots.
-spec stable(m()) -> {list(dot()), m()}.
stable(#state{node_number=NodeNumber, stable=CurrentStable, matrix=Matrix}=State)->
    NewStable = case maps:size(Matrix) of
        NodeNumber ->
            intersect_all(Matrix);
        _ ->
            %% if not enough info, bottom
            vclock:new()
    end,

    StableDots = vclock:subtract(NewStable, CurrentStable),
    {StableDots, State#state{stable=NewStable}}.

%% @private Assumes map is non-empty.
-spec intersect_all(maps:map(ldb_node_id(), vclock())) -> vclock().
intersect_all(Matrix) ->
    [{_, VV}|Rest] = maps:to_list(Matrix),
    intersect_all(VV, Rest).

-spec intersect_all(vclock(), term()) -> vclock().
intersect_all(Min0, [{_, VV}|Rest]) ->
    Min = vclock:intersection(Min0, VV),
    intersect_all(Min, Rest);
intersect_all(Min0, []) ->
    Min0.

-ifdef(TEST).

stable_test() ->
    NodeNumber = 2,
    M0 = new(NodeNumber),

    %% nodes
    A = 0,
    B = 1,

    %% dots
    A1 = {A, 1},
    A2 = {A, 2},
    B1 = {B, 1},
    B2 = {B, 2},
    B3 = {B, 3},
    B4 = {B, 4},

    %% clocks
    ClockA1 = #{A => 1, B => 1},
    ClockA2 = #{A => 1, B => 2},
    ClockB1 = #{B => 3},
    ClockA3 = #{A => 1, B => 3},
    ClockB2 = #{A => 1, B => 3},
    ClockA4 = #{A => 2, B => 4},
    ClockB3 = #{A => 2, B => 4},

    %% nothing is stable in the beg.
    ?assertEqual({[], M0}, stable(M0)),

    %% update A
    M1 = update(A, ClockA1, M0),
    ?assertEqual({[], M1}, stable(M1)),

    %% update A
    M2 = update(A, ClockA2, M1),
    ?assertEqual({[], M2}, stable(M2)),

    %% update B
    M3 = update(B, ClockB1, M2),
    {StableDots0, M4} = stable(M3),
    ?assertEqual([B1, B2], lists:sort(StableDots0)),

    %% update A
    M5 = update(A, ClockA3, M4),
    {StableDots1, M6} = stable(M5),
    ?assertEqual([B3], StableDots1),

    %% update B
    M7 = update(B, ClockB2, M6),
    {StableDots2, M8} = stable(M7),
    ?assertEqual([A1], StableDots2),

    %% update A
    M9 = update(A, ClockA4, M8),
    ?assertEqual({[], M9}, stable(M9)),

    %% update B
    M10 = update(B, ClockB3, M9),
    {StableDots3, _} = stable(M10),
    ?assertEqual([A2, B4], lists:sort(StableDots3)),
    ok.

-endif.
