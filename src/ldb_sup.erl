%%
%% Copyright (c) 2016-2018 Vitor Enes.  All Rights Reserved.
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
    BaseSpecs = configure(),

    Backend = {ldb_backend,
               {ldb_backend, start_link, []},
               permanent, 5000, worker, [ldb_backend]},

    Whisperer = {ldb_whisperer,
                 {ldb_whisperer, start_link, []},
                 permanent, 5000, worker, [ldb_whisperer]},

    Listener = {ldb_listener,
                {ldb_listener, start_link, []},
                permanent, 5000, worker, [ldb_listener]},

    ReplicationSpecs = [Backend,
                        Whisperer,
                        Listener],

    SpaceSpecs = space_specs(),

    Children = BaseSpecs ++ ReplicationSpecs ++ SpaceSpecs,

    lager:info("ldb_sup initialized!"),
    RestartStrategy = {one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.

%% @private
configure() ->
    %% configure node number
    configure_int("NODE_NUMBER",
                  node_number,
                  1),

    %% configure mode
    configure_var("LDB_MODE",
                  ldb_mode,
                  ?DEFAULT_MODE),

    %% configure driven mode
    configure_var("LDB_DRIVEN_MODE",
                  ldb_driven_mode,
                  ?DEFAULT_DRIVEN_MODE),

    %% configure state sync interval
    configure_int("LDB_STATE_SYNC_INTERVAL",
                  ldb_state_sync_interval,
                  ?DEFAULT_STATE_SYNC_INTERVAL),

    %% configure redundant delta groups
    configure_var("LDB_REDUNDANT_DGROUPS",
                  ldb_redundant_dgroups,
                  false),

    %% configure delta group back propagation
    configure_var("LDB_DGROUP_BACK_PROPAGATION",
                  ldb_dgroup_back_propagation,
                  false),

    %% configure metrics
    Metrics = configure_var("LDB_METRICS",
                            ldb_metrics,
                            true),

    BaseSpecs = case Metrics of
        true ->
            [{ldb_metrics,
              {ldb_metrics, start_link, []},
              permanent, 5000, worker, [ldb_metrics]}];
        false ->
            []
    end,

    BaseSpecs.


%% @private
space_specs() ->
    %% the space server is only started if LDB_SPACE_PORT is defined
    case list_to_integer(os:getenv("LDB_SPACE_PORT", "-1")) of
        -1 ->
            [];
        SpacePort ->
            [{ldb_space_server,
              {ldb_space_server, start_link, [SpacePort]},
              permanent, 5000, worker, [ldb_space_server]}]
    end.

%% @private
configure_var(Env, Var, Default) ->
    To = fun(V) -> atom_to_list(V) end,
    From = fun(V) -> list_to_atom(V) end,
    configure(Env, Var, Default, To, From).

%% @private
configure_int(Env, Var, Default) ->
    To = fun(V) -> integer_to_list(V) end,
    From = fun(V) -> list_to_integer(V) end,
    configure(Env, Var, Default, To, From).

%% @private
configure(Env, Var, Default, To, From) ->
    Current = ldb_config:get(Var, Default),
    Val = From(
        os:getenv(Env, To(Current))
    ),
    ldb_config:set(Var, Val),
    Val.
