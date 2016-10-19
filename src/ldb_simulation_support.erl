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
    NameToNode = start(
        [{ldb_evaluation_timestamp, timestamp()} | Options]
    ),
    construct_overlay(Options, NameToNode),
    wait_for_completion(NameToNode),
    stop(NameToNode).

%% @private Start nodes.
start(Options) ->
    ok = start_erlang_distribution(),
    Names = proplists:get_value(nodes, Options),

    InitializerFun = fun(Name, Acc) ->
        ct:pal("Starting node: ~p", [Name]),

        %% Start node
        Config = [{monitor_master, true},
                  {startup_functions, [{code, set_path, [codepath()]}]}],

        case ct_slave:start(Name, Config) of
            {ok, Node} ->
                orddict:store(Name, Node, Acc);
            Error ->
                ct:fail(Error)
        end
    end,
    NameToNode = lists:foldl(InitializerFun, orddict:new(), Names),
    Nodes = [Node || {_Name, Node} <- orddict:to_list(NameToNode)],

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
    lists:foreach(LoaderFun, Nodes),

    ConfigureFun = fun(Node) ->
        ct:pal("Configuring node: ~p", [Node]),

        %% Set mode
        Mode = proplists:get_value(ldb_mode, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_mode, Mode]),

        %% Set join decompositions
        JoinDecompositions = proplists:get_value(ldb_join_decompositions, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_join_decompositions, JoinDecompositions]),

        %% Set simulation
        Simulation = proplists:get_value(ldb_simulation, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_simulation, Simulation]),

        %% Set instrumentation
        Instrumentation = true,
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_instrumentation, Instrumentation]),

        %% Set extended logging
        ExtendedLogging = true,
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_extended_logging, ExtendedLogging]),

        %% Set client number
        ClientNumber = length(Names),
        ok = rpc:call(Node,
                      application, set_env,
                      [?APP, client_number, ClientNumber]),

        %% Set evaluation identifier
        EvaluationIdentifier = proplists:get_value(ldb_evaluation_identifier, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_evaluation_identifier, EvaluationIdentifier]),

        %% Set evaluation timestamp
        EvaluationTimestamp = proplists:get_value(ldb_evaluation_timestamp, Options),
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_evaluation_timestamp, EvaluationTimestamp])

    end,
    lists:foreach(ConfigureFun, Nodes),

    StartFun = fun(Node) ->
        {ok, _} = rpc:call(Node,
                           application,
                           ensure_all_started,
                           [?APP])
    end,
    lists:foreach(StartFun, Nodes),

    NameToNode.

%% @private Connect each node to its peers.
construct_overlay(Options, NameToNode) ->
    Graph = proplists:get_value(graph, Options),

    NameToNodeSpec = lists:map(
        fun({Name, Node}) ->
            {ok, Info} = rpc:call(Node, ldb_peer_service, get_node_info, []),
            {Name, Info}
        end,
        NameToNode
    ),

    ct:pal("Graph ~n~p~n", [Graph]),
    ct:pal("Nodes ~n~p~n", [NameToNode]),
    ct:pal("Nodes Info ~n~p~n", [NameToNodeSpec]),

    lists:foreach(
        fun({Name, Peers}) ->
            lists:foreach(
                fun(PeerName) ->
                    Node = orddict:fetch(Name, NameToNode),
                    PeerSpec = orddict:fetch(PeerName, NameToNodeSpec),

                    ct:pal("Node ~p~n~n", [Node]),
                    ct:pal("PeerSpec ~p~n~n", [PeerSpec]),

                    ok = rpc:call(Node,
                                  ldb_peer_service,
                                  join,
                                  [PeerSpec])
                end,
                Peers
            )
        end,
        Graph
    ).

%% @private Poll nodes to see if simulation is ended.
wait_for_completion(NameToNode) ->
    ct:pal("Waiting for simulation to end"),

    Result = ldb_util:wait_until(
        fun() ->
            lists:foldl(
                fun({_Name, Node}, Acc) ->
                    SimulationEnd = rpc:call(Node,
                                             application,
                                             get_env,
                                             [?APP, simulation_end, false]),
                    ct:pal("Node ~p with simulation end as ~p", [Node, SimulationEnd]),
                    Acc andalso SimulationEnd
                end,
                true,
                NameToNode
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
stop(NameToNode) ->
    StopFun = fun({Name, _Node}) ->
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:foreach(StopFun, NameToNode).

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

%% @private
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.
