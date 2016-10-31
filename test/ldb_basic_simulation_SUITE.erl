%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(ldb_basic_simulation_SUITE).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include("ldb.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning test case: ~p", [Case]),
    Config.

end_per_testcase(Case, Config) ->
    ct:pal("Ending test case: ~p", [Case]),
    Config.

all() ->
    [
     %state_based_test,
     delta_based_test,
     join_decompositions_test,
     pure_op_based_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

state_based_test(_Config) ->
    run(state_based).

delta_based_test(_Config) ->
    run(delta_based).

join_decompositions_test(_Config) ->
    run(join_decompositions).

pure_op_based_test(_Config) ->
    run(pure_op_based).

%% @private
run(Evaluation) ->
    Nodes = node_names(),
    lists:foreach(
        fun({Topology, Graph}) ->
            {Mode, JoinDecompositions} =
                get_mode_and_join_decompositions(Evaluation),

            Identifier = list_to_atom(
                atom_to_list(Evaluation)
                ++ "_"
                ++ atom_to_list(Topology)
            ),
            Options = [{nodes, Nodes},
                       {graph, Graph},
                       {ldb_mode, Mode},
                       {ldb_join_decompositions, JoinDecompositions},
                       {ldb_simulation, basic},
                       {ldb_evaluation_identifier, Identifier}],
            ldb_simulation_support:run(Options)
        end,
        topologies()
    ).

%% @private
get_mode_and_join_decompositions(state_based) ->
    {state_based, false};
get_mode_and_join_decompositions(delta_based) ->
    {delta_based, false};
get_mode_and_join_decompositions(join_decompositions) ->
    {delta_based, true};
get_mode_and_join_decompositions(pure_op_based) ->
    {pure_op_based, false}.

%% @private
node_names() ->
    case os:getenv("TRAVIS", "false") of
        "false" ->
            lists:seq(0, 12);
        "true" ->
            lists:seq(0, 2)
    end.

%% @private
topologies() ->
    case os:getenv("TRAVIS", "false") of
        "false" ->
            [{ring, ldb_overlay:get(ring, 13)},
             {hyparview, ldb_overlay:get(hyparview, 13)},
             {erdos_renyi, ldb_overlay:get(erdos_renyi, 13)}];
        "true" ->
            [{line, ldb_overlay:get(line, 3)}]
    end.
