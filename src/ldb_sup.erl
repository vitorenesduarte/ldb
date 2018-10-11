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
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(Name, Mod, Args),
        {Name, {Mod, start_link, Args}, permanent, 5000, worker, [Mod]}).
-define(CHILD(I),
        ?CHILD(I, I, [])).

-define(LISTENER_OPTIONS(Port),
        [{port, Port},
         %% we probably won't have more than 32 servers
         {max_connections, 32},
         %% one acceptor should be enough since the number
         %% of servers is small
         {num_acceptors, 1}]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Port = configure(),

    %% start server tcp acceptor
    {ok, _} = ranch:start_listener(ldb_hao_server,
                                   ranch_tcp,
                                   ?LISTENER_OPTIONS(Port),
                                   ldb_hao_client,
                                   []),

    %% start metrics
    ldb_metrics:start(),

    %% hao
    BaseSpecs = [?CHILD(ldb_hao)],

    %% all the shards
    ReplicationSpecs = [?CHILD(Shard, ldb_shard, [Shard])
                        || Shard <- ldb_forward:all_shards()],

    Children = BaseSpecs ++ ReplicationSpecs,

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
    configure_var("LDB_METRICS",
                  ldb_metrics,
                  true),

    %% configure server ip
    configure_ip("LDB_IP",
                 ldb_ip,
                 {127, 0, 0, 1}),

    %% configure server port
    Port = configure_int("LDB_PORT",
                         ldb_port,
                         5000),
    Port.

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
configure_ip(Env, Var, Default) ->
    To = fun(V) -> inet_parse:ntoa(V) end,
    From = fun(V) -> {ok, IP} = inet_parse:address(V), IP end,
    configure(Env, Var, Default, To, From).

%% @private
configure(Env, Var, Default, To, From) ->
    Current = ldb_config:get(Var, Default),
    Val = From(
        os:getenv(Env, To(Current))
    ),
    ldb_config:set(Var, Val),
    Val.
