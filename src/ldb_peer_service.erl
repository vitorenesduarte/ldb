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

-export([members/0,
         join/1,
         forward_message/3,
         get_node_spec/0]).

%% @doc Return a list of neighbors
-callback members() -> {ok, [ldb_node_id()]}.

%% @doc Attempt to join node.
-callback join(node_spec()) -> ok | error().

%% @doc Send a message to a node.
-callback forward_message(ldb_node_id(), handler(), message()) ->
    ok | error().

%% @doc Retrieves the node spec: {name, ip, port}
-callback get_node_spec() -> {ok, node_spec()}.

-spec members() -> {ok, [ldb_node_id()]}.
members() ->
    case peer_service_defined() of
        true ->
            do(members, []);
        false ->
            {ok, []}
    end.

-spec join(node_spec()) -> ok | error().
join(NodeSpec) ->
    do(join, [NodeSpec]).

-spec forward_message(ldb_node_id(), handler(), message()) ->
    ok | error().
forward_message(LDBId, Handler, Message) ->
    do(forward_message, [LDBId, Handler, Message]).

-spec get_node_spec() -> {ok, node_spec()}.
get_node_spec() ->
    do(get_node_spec, []).

%% @private Execute call to the proper peer service.
do(Function, Args) ->
    PeerService = ldb_config:get(ldb_peer_service),
    erlang:apply(PeerService, Function, Args).

%% @private check if we have a peer service defined.
peer_service_defined() ->
    ldb_config:get(ldb_peer_service, undefined) /= undefined.
