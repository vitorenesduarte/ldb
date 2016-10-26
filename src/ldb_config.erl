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

-module(ldb_config).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([mode/0,
         backend/0,
         store/0,
         peer_service/0,
         node_number/0,
         extended_logging/0]).

%% @doc Returns the enabled mode.
%%      The result can be:
%%          - `state_based'
%%          - `delta_based'
%%          - `pure_op_based'
-spec mode() -> atom().
mode() ->
    application:get_env(?APP, ldb_mode, ?DEFAULT_MODE).

%% @doc Returns the enabled backend.
-spec backend() -> atom().
backend() ->
    case mode() of
        state_based ->
            ldb_state_based_backend;
        delta_based ->
            ldb_delta_based_backend
    end.

%% @doc Returns the enabled store.
-spec store() -> atom().
store() ->
    application:get_env(?APP, ldb_store, ?DEFAULT_STORE).

%% @doc Returns the enabled peer service.
-spec peer_service() -> atom().
peer_service() ->
    application:get_env(?APP, ldb_peer_service, ?DEFAULT_PEER_SERVICE).

%% @doc Returns the node number.
-spec node_number() -> non_neg_integer().
node_number() ->
    {ok, NodeNumber} = application:get_env(?APP, ldb_node_number),
    NodeNumber.

%% @doc Returns true if extended logging is enabled.
-spec extended_logging() -> atom().
extended_logging() ->
    application:get_env(?APP, ldb_extended_logging, false).
