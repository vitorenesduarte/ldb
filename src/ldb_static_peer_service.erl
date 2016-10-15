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

-record(state, {node_info :: node_info(),
                connections :: ordsets:ordset(node_info())}).

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
    Info = init_node_info(),
    schedule_log(),

    lager:info("ldb_static_peer_service initialized!"),
    {ok, #state{node_info=Info, connections=ordsets:new()}}.

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

handle_call(get_node_info, _From, #state{node_info=Info}=State) ->
    Result = {ok, Info},
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
init_node_info() ->
    Name = node(),
    IP = case os:getenv("PEER_IP", undefined) of
        undefined ->
            {127, 0, 0, 1};
        PeerIP ->
            {ok, IPAddress} = inet_parse:address(PeerIP),
            IPAddress
    end,
    Port = case os:getenv("PEER_PORT", undefined) of
        undefined ->
            random_port();
        PeerPort ->
            list_to_integer(PeerPort)
    end,

    {Name, IP, Port}.

random_port() ->
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),
    rand_compat:uniform(1000) + 10000.

%% @private
schedule_log() ->
    timer:send_after(?LOG_INTERVAL, log).
