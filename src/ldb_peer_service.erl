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

-module(ldb_peer_service).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([start_link/0,
         members/0,
         join/1,
         forward_message/3,
         get_node_info/0]).

%% @doc Return a list of neighbors
-callback members() -> {ok, [node_name()]}.

%% @doc Attempt to join node.
-callback join(node_info()) -> ok | error().

%% @doc Send a message to a node.
-callback forward_message(node_name(), handler(), message()) ->
    ok | error().

%% @doc Retrieves the node info: {name, ip, port}
-callback get_node_info() -> {ok, node_info()}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec members() -> {ok, [node_name()]}.
members() ->
    do(members, []).

-spec join(node_info()) -> ok | error().
join(NodeSpec) ->
    do(join, [NodeSpec]).

-spec forward_message(node_name(), handler(), message()) ->
    ok | error().
forward_message(Name, Handler, Message) ->
    do(forward_message, [Name, Handler, Message]).

-spec get_node_info() -> {ok, node_info()}.
get_node_info() ->
    do(get_node_info, []).

%% @private Execute call to the proper peer service.
do(Function, Args) ->
    Store = ldb_config:peer_service(),
    erlang:apply(Store, Function, Args).
