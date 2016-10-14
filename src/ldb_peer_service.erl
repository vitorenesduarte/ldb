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

-module(ldb_peer_service).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([start_link/0,
         members/0,
         join/1,
         forward_message/3]).

%% @doc Return a list of neighbors
-callback members() -> {ok, [node()]}.

%% @doc Attempt to join node.
-callback join(node_spec()) -> ok | error().

%% @doc Send a message to a node.
%%      The process with the ref passed as argument should
%%      handle the replies.
-callback forward_message(node(), pid(), message()) -> ok.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    do(start_link, []).

-spec members() -> {ok, [node()]}.
members() ->
    do(members, []).

-spec join(node_spec()) -> ok | error().
join(NodeSpec) ->
    do(join, [NodeSpec]).

-spec forward_message(node(), pid(), message()) -> ok.
forward_message(Node, Ref, Message) ->
    do(forward_message, [Node, Ref, Message]).

%% @private Execute call to the proper peer service.
do(Function, Args) ->
    Store = ldb_util:peer_service(),
    erlang:apply(Store, Function, Args).
