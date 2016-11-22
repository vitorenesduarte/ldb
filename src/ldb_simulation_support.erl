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
    IdToNode = start(
        [{ldb_evaluation_timestamp, timestamp()} | Options]
    ),
    construct_overlay(Options, IdToNode),
    wait_for_completion(IdToNode),
    stop(IdToNode).

%% @private Start nodes.
start(Options) ->
    ok = start_erlang_distribution(),
    Ids = proplists:get_value(nodes, Options),

    InitializerFun = fun(Id, Acc) ->
        Name = get_name_from_id(Id),
        ct:pal("Starting node: ~p", [Name]),

        %% Start node
        Config = [{monitor_master, true},
                  {startup_functions, [{code, set_path, [codepath()]}]}],

        case ct_slave:start(Name, Config) of
            {ok, Node} ->
                orddict:store(Id, Node, Acc);
            Error ->
                ct:fail(Error)
        end
    end,
    IdToNode = lists:foldl(InitializerFun, orddict:new(), Ids),
    Nodes = [Node || {_Id, Node} <- orddict:to_list(IdToNode)],

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

    ConfigureFun = fun({Id, Node}) ->
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

        %% Set node number
        NodeNumber = length(Ids),
        ok = rpc:call(Node,
                      application, set_env,
                      [?APP, ldb_node_number, NodeNumber]),

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
                      [?APP, ldb_evaluation_timestamp, EvaluationTimestamp]),

        %% Set id
        ok = rpc:call(Node,
                      application,
                      set_env,
                      [?APP, ldb_id, Id])

    end,
    lists:foreach(ConfigureFun, IdToNode),

    StartFun = fun(Node) ->
        {ok, _} = rpc:call(Node,
                           application,
                           ensure_all_started,
                           [?APP])
    end,
    lists:foreach(StartFun, Nodes),

    IdToNode.

%% @private Connect each node to its peers.
construct_overlay(Options, IdToNode) ->
    Graph = proplists:get_value(graph, Options),

    IdToNodeSpec = lists:map(
        fun({Id, Node}) ->
            {ok, Info} = rpc:call(Node, ldb_peer_service, get_node_info, []),
            {Id, Info}
        end,
        IdToNode
    ),

    ct:pal("Graph ~n~p~n", [Graph]),
    ct:pal("Nodes ~n~p~n", [IdToNode]),
    ct:pal("Nodes Info ~n~p~n", [IdToNodeSpec]),

    lists:foreach(
        fun({Id, PeersId}) ->
            Node = orddict:fetch(Id, IdToNode),
            ct:pal("Node ~p~n~n", [Node]),

            lists:foreach(
                fun(PeerId) ->
                    PeerSpec = orddict:fetch(PeerId, IdToNodeSpec),

                    ct:pal("PeerSpec ~p~n~n", [PeerSpec]),

                    ok = rpc:call(Node,
                                  ldb_peer_service,
                                  join,
                                  [PeerSpec])
                end,
                PeersId
            )
        end,
        Graph
    ).

%% @private Poll nodes to see if simulation is ended.
wait_for_completion(IdToNode) ->
    ct:pal("Waiting for simulation to end"),

    Result = ldb_util:wait_until(
        fun() ->
            lists:foldl(
                fun({_Id, Node}, Acc) ->
                    SimulationEnd = rpc:call(Node,
                                             application,
                                             get_env,
                                             [?APP, simulation_end, false]),
                    ct:pal("Node ~p with simulation end as ~p", [Node, SimulationEnd]),
                    Acc andalso SimulationEnd
                end,
                true,
                IdToNode
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
stop(IdToNode) ->
    StopFun = fun({Id, _Node}) ->
        Name = get_name_from_id(Id),
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:foreach(StopFun, IdToNode).

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
    Timestamp = Mega * 1000000 + Sec,
    TaskId = "local",
    list_to_atom(TaskId ++ "_" ++ integer_to_list(Timestamp)).

%% @private
get_name_from_id(Id) ->
    list_to_atom("n" ++ integer_to_list(Id)).
