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

-module(ldb_simulation_support).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([run/1]).

run(Options) ->
    {Names, Nodes} = start(Options),
    wait_for_completion(Nodes),
    stop(Names).

%% @private Start nodes.
start(Options) ->
    ok = start_erlang_distribution(),
    Names = proplists:get_value(nodes, Options),

    InitializerFun = fun(Name) ->
        ct:pal("Starting node: ~p", [Name]),

        %% Start node
        Config = [{monitor_master, true},
                  {startup_functions, [{code, set_path, [codepath()]}]}],

        case ct_slave:start(Name, Config) of
            {ok, Node} ->
                Node;
            Error ->
                ct:fail(Error)
        end
    end,
    Nodes = lists:map(InitializerFun, Names),

    LoaderFun = fun(Node) ->
        ct:pal("Loading ldb on node: ~p", [Node]),

        %% Load ldb
        ok = rpc:call(Node, application, load, [?APP]),

        %% Set lager log dir
        PrivDir = code:priv_dir(?APP),
        NodeDir = filename:join([PrivDir, "lager", Node]),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [lager, log_root, NodeDir])
    end,
    lists:map(LoaderFun, Nodes),

    ConfigureFun = fun(Node) ->
        ct:pal("Configuring node: ~p", [Node]),

        %% Set simulation
        Simulation = proplists:get_value(simulation, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, simulation, Simulation])
    end,
    lists:map(ConfigureFun, Nodes),

    StartFun = fun(Node) ->
        {ok, _} = rpc:call(Node,
                           application,
                           ensure_all_started,
                           [?APP])
    end,
    lists:map(StartFun, Nodes),

    {Names, Nodes}.

%% @private Poll nodes to see if simulation is ended.
wait_for_completion(Nodes) ->
    ct:pal("Waiting for simulation to end"),

    Result = ldb_util:wait_until(
        fun() ->
            lists:foldl(
                fun(Node, Acc) ->
                    SimulationEnd = rpc:call(Node,
                                             application,
                                             get_env,
                                             [?APP, simulation_end, false]),
                    ct:pal("Node ~p with simulation end as ~p", [Node, SimulationEnd]),
                    Acc andalso SimulationEnd
                end,
                true,
                Nodes
             )
        end,
        100,      %% 100 retries
        10 * 1000 %% every 10 seconds
    ),

    case Result of
        ok ->
            ct:pal("Simulation ended with success");
        fail ->
            ct:fail("Simulation failed")
    end.

%% @private Stop nodes.
stop(Names) ->
    StopFun = fun(Name) ->
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:map(StopFun, Names).

%% @private Start erlang distribution.
start_erlang_distribution() ->
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end.

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).
