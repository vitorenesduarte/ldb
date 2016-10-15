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

-module(ldb_static_peer_service).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_peer_service).
-behaviour(gen_server).

%% ldb_static_peer_service callbacks
-export([start_link/0,
         members/0,
         join/1,
         forward_message/3,
         get_node_info/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {connections :: ordsets:ordset(node_info())}).

-define(LOG_INTERVAL, 5000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec members() -> {ok, [node_info()]}.
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec join(node_info()) -> ok | error().
join(NodeSpec) ->
    gen_server:call(?MODULE, {join, NodeSpec}, infinity).

-spec forward_message(node_info(), pid(), message()) -> ok.
forward_message(Info, Ref, Message) ->
    gen_server:call(?MODULE, {forward_message, Info, Ref, Message}, infinity).

-spec get_node_info() -> {ok, node_info()}.
get_node_info() ->
    gen_server:call(?MODULE, get_node_info, infinity).

%% gen_server callbacks
init([]) ->
    schedule_log(),
    lager:info("ldb_static_peer_service initialized!"),
    {ok, #state{connections=ordsets:new()}}.

handle_call(members, _From, #state{connections=Connections}=State) ->
    Result = {ok, ordsets:to_list(Connections)},
    {reply, Result, State};

handle_call({join, {_Name, {_, _, _, _}=_IpAddres, _Port}=_Spec}, _From,
            #state{connections=_Connections}=State) ->
    Result = ok,
    {reply, Result, State};

handle_call({forward_message, _Info, _Ref, _Message}, _From, State) ->
    Result = ok,
    {reply, Result, State};

handle_call(get_node_info, _From, State) ->
    Result = {ok, {name, {127, 0, 0, 1}, 8765}},
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(log, #state{connections=Connections}=State) ->
    lager:info("Current connections ~p | Node ~p", [Connections, node()]),
    schedule_log(),
    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_log() ->
    timer:send_after(?LOG_INTERVAL, log).
