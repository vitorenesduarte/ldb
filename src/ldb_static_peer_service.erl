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
                connected :: orddict:orddict()}).

-define(LOG_INTERVAL, 10000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec members() -> {ok, [node_name()]}.
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec join(node_info()) -> ok | error().
join(NodeInfo) ->
    lager:info("\n\n\nJOIN RECEIVED ON NODE ~p to NODE ~p\n\n\n", [node(), NodeInfo]),
    gen_server:call(?MODULE, {join, NodeInfo}, infinity).

-spec forward_message(node_name(), handler(), message()) ->
    ok | error().
forward_message(Name, Handler, Message) ->
    gen_server:call(?MODULE, {forward_message, Name, Handler, Message}, infinity).

-spec get_node_info() -> {ok, node_info()}.
get_node_info() ->
    gen_server:call(?MODULE, get_node_info, infinity).

%% gen_server callbacks
init([]) ->
    NodeInfo = init_node_info(),
    {ok, _} = ldb_static_peer_service_server:start_link(NodeInfo),
    schedule_log(),

    ldb_log:info("ldb_static_peer_service initialized!", extended),
    {ok, #state{node_info=NodeInfo, connected=orddict:new()}}.

handle_call(members, _From, #state{connected=Connected}=State) ->
    Result = {ok, orddict:fetch_keys(Connected)},
    {reply, Result, State};

handle_call({join, {Name, {_, _, _, _}=_Ip, _Port}=NodeInfo}, _From,
            #state{connected=Connected0}=State) ->
    {Result, Connected1} = case orddict:find(Name, Connected0) of
        {ok, _} ->
            {ok, Connected0};
        error ->
            lager:info("\n\n\nNOT CONNECTED YET\n\n\n"),
            case ldb_static_peer_service_client:start_link(NodeInfo) of
                {ok, Pid} ->
                    {ok, orddict:store(Name, Pid, Connected0)};
                Error ->
                    ldb_log:info("Error handling join call on node ~p to node ~p. Reason ~p", [node(), NodeInfo, Error]),
                    {Error, Connected0}
            end
    end,
    {reply, Result, State#state{connected=Connected1}};

handle_call({forward_message, Name, Handler, Message}, _From, #state{connected=Connected}=State) ->
    Result = case orddict:find(Name, Connected) of
        {ok, Pid} ->
            Pid ! {forward_message, Handler, Message},
            ok;
        error ->
            {error, not_connected}
    end,

    {reply, Result, State};

handle_call(get_node_info, _From, #state{node_info=NodeInfo}=State) ->
    Result = {ok, NodeInfo},
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(log, #state{connected=Connected}=State) ->
    NodeNames = orddict:fetch_keys(Connected),
    ldb_log:info("Current connected nodes ~p | Node ~p", [NodeNames, node()], extended),
    schedule_log(),
    {noreply, State};

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
init_node_info() ->
    Name = node(),
    IP = case os:getenv("PEER_IP", "undefined") of
        "undefined" ->
            {127, 0, 0, 1};
        PeerIP ->
            {ok, IPAddress} = inet_parse:address(PeerIP),
            IPAddress
    end,
    Port = case os:getenv("PEER_PORT", "undefined") of
        "undefined" ->
            random_port();
        PeerPort ->
            list_to_integer(PeerPort)
    end,

    lager:info("NODE INFO ~p", [{Name, IP, Port}]),
    {Name, IP, Port}.

random_port() ->
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),
    rand_compat:uniform(10000) + 3000.

%% @private
schedule_log() ->
    timer:send_after(?LOG_INTERVAL, log).
