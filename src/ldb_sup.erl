%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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
    configure(),

    Backend = {ldb_backend,
               {ldb_backend, start_link, []},
               permanent, 5000, worker, [ldb_backend]},

    Whisperer = {ldb_whisperer,
                 {ldb_whisperer, start_link, []},
                 permanent, 5000, worker, [ldb_whisperer]},

    Listener = {ldb_listener,
                {ldb_listener, start_link, []},
                permanent, 5000, worker, [ldb_listener]},

    SpaceServer = {ldb_space_server,
                   {ldb_space_server, start_link, []},
                   permanent, 5000, worker, [ldb_space_server]},

    Children = [Backend,
                Whisperer,
                Listener,
                SpaceServer],

    ldb_log:info("ldb_sup initialized!"),
    RestartStrategy = {one_for_one, 5, 10},
    {ok, {RestartStrategy, Children}}.

%% @private
configure() ->
    %% Configure mode
    case list_to_atom(os:getenv("LDB_MODE", "undefined")) of
        undefined ->
            ok;
        Mode ->
            ldb_config:set(ldb_mode, Mode)
    end,

    %% Configure space server
    case list_to_integer(os:getenv("LDB_SPACE_PORT", "-1")) of
        -1 ->
            ok;
        SpacePort ->
            ldb_config:set(ldb_space_port, SpacePort)
    end.
