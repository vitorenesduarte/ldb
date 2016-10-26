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

-module(ldb_sup).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Configure peer service
    PeerService = list_to_atom(os:getenv("LDB_PEER_SERVICE", "undefined")),
    case PeerService /= undefined of
        true ->
            application:set_env(?APP,
                                ldb_peer_service,
                                PeerService);
        false ->
            ok
    end,

    %% Start peer service
    {ok, _} = ldb_peer_service:start_link(),


    %% Configure node number
    NodeNumberDefault = list_to_integer(os:getenv("LDB_NODE_NUMBER", "-1")),
    NodeNumber = application:get_env(?APP, ldb_node_number, NodeNumberDefault),
    case NodeNumber of
        -1 ->
            ok;
        _ ->
            application:set_env(?APP,
                                ldb_node_number,
                                NodeNumber)
    end,

    %% If running in DCOS, create overlay
    ok = case os:getenv("DCOS", "undefined") of
        "undefined" ->
            ok;
        _ ->
            %%ldb_dcos:create_overlay()
            ok
    end,

    %% Configure mode
    Mode = list_to_atom(os:getenv("LDB_MODE", "undefined")),
    case Mode /= undefined of
        true ->
            application:set_env(?APP,
                                ldb_mode,
                                Mode);
        false ->
            ok
    end,

    {ok, _} = ldb_backend:start_link(),
    {ok, _} = ldb_whisperer:start_link(),
    {ok, _} = ldb_listener:start_link(),

    %% Configure space server
    SpaceServerPortDefault = list_to_integer(os:getenv("LDB_PORT", "-1")),
    SpaceServerPort = application:get_env(?APP,
                                          ldb_port,
                                          SpaceServerPortDefault),
    case SpaceServerPort of
        -1 ->
            %% don't start the space server
            ok;
        _ ->
            {ok, _} = ldb_space_server:start_link(SpaceServerPort)
    end,

    %% Configure simulation
    SimulationDefault = list_to_atom(os:getenv("LDB_SIMULATION", "undefined")),
    Simulation = application:get_env(?APP,
                                     ldb_simulation,
                                     SimulationDefault),

    case Simulation of
        basic ->
            {ok, _} = ldb_basic_simulation:start_link();
        undefined ->
            ok
    end,

    %% Configure instrumentation
    InstrumentationDefault = list_to_atom(os:getenv("LDB_INSTRUMENTATION", "false")),
    Instrumentation = application:get_env(?APP,
                                          ldb_instrumentation,
                                          InstrumentationDefault),

    case Instrumentation of
        true ->
            {ok, _} = ldb_instrumentation:start_link();
        false ->
            ok
    end,

    application:set_env(?APP,
                        ldb_instrumentation,
                        Instrumentation),

    %% Configure extended logging
    ExtendedLogging = list_to_atom(os:getenv("LDB_EXTENDED_LOGGING", "false")),
    case ExtendedLogging /= undefined of
        true ->
            application:set_env(?APP,
                                ldb_extended_logging,
                                ExtendedLogging);
        false ->
            ok
    end,

    ldb_log:info("ldb_sup initialized!"),
    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, []}}.

