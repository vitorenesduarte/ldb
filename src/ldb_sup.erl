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

-behaviour(supervisor).

-include("ldb.hrl").

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, _} = ldb_peer_service:start_link(),
    {ok, _} = ldb_backend:start_link(),
    {ok, _} = ldb_gossip_girl:start_link(),

    %% Configure simulation
    SimulationDefault = list_to_atom(os:getenv("SIMULATION", "undefined")),
    Simulation = application:get_env(?APP,
                                     simulation,
                                     SimulationDefault),

    case Simulation of
        basic ->
            ldb_basic_simulation:start_link();
        undefined ->
            ok
    end,

    lager:info("ldb_sup initialized!"),
    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, []}}.

