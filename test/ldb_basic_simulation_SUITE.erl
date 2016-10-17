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
     state_based_test,
     delta_based_test
    ].

%% ===================================================================
%% tests
%% ===================================================================

state_based_test(_Config) ->
    Nodes = [n1, n2, n3, n4, n5],
    %% This graph forms a line
    Graph = [{n1, [n2]},
             {n2, [n1, n3]},
             {n3, [n2, n4]},
             {n4, [n3, n5]},
             {n5, [n4]}],
    Options = [{nodes, Nodes},
               {graph, Graph},
               {ldb_mode, state_based},
               {ldb_simulation, basic}],
    ldb_simulation_support:run(Options).

delta_based_test(_Config) ->
    Nodes = [n1, n2, n3, n4, n5],
    %% This graph forms a line
    Graph = [{n1, [n2]},
             {n2, [n1, n3]},
             {n3, [n2, n4]},
             {n4, [n3, n5]},
             {n5, [n4]}],
    Options = [{nodes, Nodes},
               {graph, Graph},
               {ldb_mode, delta_based},
               {ldb_simulation, basic}],
    ldb_simulation_support:run(Options).
