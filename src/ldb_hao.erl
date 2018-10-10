%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
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

%% @doc hao it's a peer service.
%% TODO find a better name.

-module(ldb_hao).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0,
         myself/0,
         join/1,
         members/0,
         forward_message/3,
         exit/2]).

%% gen_server
-export([init/1,
         handle_call/3,
         handle_cast/2]).

-record(state, {connections :: ldb_hao_connections:connections()}).

-spec start_link() -> {ok, pid()} | ignore | error().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec myself() -> node_spec().
myself() ->
    Id = ldb_config:id(),
    Ip = ldb_config:get(ldb_ip),
    Port = ldb_config:get(ldb_port),
    {Id, Ip, Port}.

-spec join(node_spec()) -> ok | error().
join(Spec) ->
    gen_server:call(?MODULE, {join, Spec}, infinity).

-spec members() -> {ok, list(ldb_node_id())}.
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec forward_message(ldb_node_id(), atom(), message()) -> ok.
forward_message(Id, Mod, Message) ->
    %% pick a random connection
    Name = ldb_util:connection_name(Id),
    gen_server:cast(Name, {forward_message, Mod, Message}).

-spec exit(ldb_node_id(), pid()) -> ok.
exit(Id, Pid) ->
    gen_server:cast(?MODULE, {exit, Id, Pid}).

init([]) ->
    lager:info("ldb hao initialized!"),

    %% create connections
    Connections = ldb_hao_connections:new(),

    {ok, #state{connections=Connections}}.

handle_call({join, {Id, Ip, Port}=Spec}, _From,
            #state{connections=Connections0}=State) ->

    lager:info("Will connect to ~p", [Spec]),

    %% try to connect
    {Result, Connections} = ldb_hao_connections:connect(Id, Ip, Port,
                                                        Connections0),

    notify_app(true, Connections),

    {reply, Result, State#state{connections=Connections}};

handle_call(members, _From, #state{connections=Connections}=State) ->
    Members = ldb_hao_connections:members(Connections),
    {reply, {ok, Members}, State}.

handle_cast({exit, Id, Pid}, #state{connections=Connections0}=State) -> 
    lager:info("EXIT of ~p.", [Id]),

    {NewMembers, Connections} = ldb_hao_connections:exit(Id, Pid, Connections0),

    notify_app(NewMembers, Connections),

    {noreply, State#state{connections=Connections}}.

%% @private
-spec notify_app(boolean(), ldb_hao_connections:connections()) -> ok.
notify_app(false, _) ->
    ok;
notify_app(true, Connections) ->
    Members = ldb_hao_connections:members(Connections),
    ldb_forward:update_members(Members).
