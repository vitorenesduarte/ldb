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
    configure_var(ldb_peer_service,
                  "LDB_PEER_SERVICE",
                  ?DEFAULT_PEER_SERVICE),

    %% Configure store
    configure_var(ldb_store,
                  "LDB_STORE",
                  ?DEFAULT_STORE),

    %% Configure node number
    configure_int(ldb_node_number,
                  "LDB_NODE_NUMBER",
                  "1"),

    %% Configure extended logging
    configure_var(ldb_extended_logging,
                  "LDB_EXTENDED_LOGGING",
                  "false"),

    %% Start peer service
    {ok, _} = ldb_peer_service:start_link(),

    %% Configure DCOS url
    configure_str(ldb_dcos_url,
                  "DCOS",
                  "undefined"),

    %% If running in DCOS, create overlay
    LDBId = case ldb_config:dcos() of
        true ->
            %% Configure DCOS token
            configure_str(ldb_dcos_token,
                          "TOKEN",
                          "undefined"),

            %% Configure DCOS overlay
            Overlay = configure_var(ldb_dcos_overlay,
                                    "LDB_DCOS_OVERLAY",
                                    "undefined"),
            ldb_dcos:create_overlay(Overlay);
        false ->
            0
    end,

    %% Configure ldb id
    configure_int(ldb_id,
                  "LDB_ID",
                  integer_to_list(LDBId)),

    %% Configure mode
    configure_var(ldb_mode,
                  "LDB_MODE",
                  ?DEFAULT_MODE),

    %% Configure join decompositions
    configure_var(ldb_join_decompositions,
                  "LDB_JOIN_DECOMPOSITIONS",
                  "false"),

    {ok, _} = ldb_backend:start_link(),
    {ok, _} = ldb_whisperer:start_link(),
    {ok, _} = ldb_listener:start_link(),

    %% Configure space server
    SpaceServerPort = configure_int(ldb_port,
                                    "LDB_PORT",
                                    "-1"),

    case SpaceServerPort of
        -1 ->
            %% don't start the space server
            ok;
        _ ->
            {ok, _} = ldb_space_server:start_link(SpaceServerPort)
    end,

    %% Configure simulation
    Simulation = configure_var(ldb_simulation,
                               "LDB_SIMULATION",
                               "undefined"),
    case Simulation of
        basic ->
            {ok, _} = ldb_basic_simulation:start_link();
        undefined ->
            ok
    end,

    %% Configure evaluation identifier
    configure_var(ldb_evaluation_identifier,
                  "LDB_EVALUATION_IDENTIFIER",
                  "undefined"),

    %% Configure evaluation timestamp
    configure_var(ldb_evaluation_timestamp,
                  "LDB_EVALUATION_TIMESTAMP",
                  "undefined"),

    %% Configure instrumentation
    Instrumentation = configure_var(ldb_instrumentation,
                                    "LDB_INSTRUMENTATION",
                                    "false"),
    case Instrumentation of
        true ->
            {ok, _} = ldb_instrumentation:start_link();
        false ->
            ok
    end,

    ldb_log:info("ldb_sup initialized!"),
    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, []}}.


%% @private
configure(LDBVariable, EnvironmentVariable, EnvironmentDefault, ParseFun) ->
    Default = ParseFun(
        os:getenv(EnvironmentVariable, EnvironmentDefault)
    ),
    Value = application:get_env(?APP,
                                LDBVariable,
                                Default),
    application:set_env(?APP,
                        LDBVariable,
                        Value),
    Value.
configure_var(LDBVariable, EnvironmentVariable, EnvironmentDefault) ->
    configure(LDBVariable, EnvironmentVariable, EnvironmentDefault, fun(V) -> list_to_atom(V) end).
configure_str(LDBVariable, EnvironmentVariable, EnvironmentDefault) ->
    configure(LDBVariable, EnvironmentVariable, EnvironmentDefault, fun(V) -> V end).
configure_int(LDBVariable, EnvironmentVariable, EnvironmentDefault) ->
    configure(LDBVariable, EnvironmentVariable, EnvironmentDefault, fun(V) -> list_to_integer(V) end).
