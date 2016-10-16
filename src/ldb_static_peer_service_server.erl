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

-module(ldb_static_peer_service_server).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_static_peer_service_server callbacks
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {listener :: gen_tcp:socket()}).

-define(LOG_INTERVAL, 5000).

-spec start_link(node_info()) -> {ok, pid()} | ignore | {error, term()}.
start_link(NodeInfo) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NodeInfo], []).

%% gen_server callbacks
init([{_Name, _IpAddress, Port}]) ->
    {ok, Listener} = gen_tcp:listen(Port, ?TCP_OPTIONS),

    accept_one(),

    lager:info("ldb_static_peer_service_server initialized!"),
    {ok, #state{listener=Listener}}.

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(accept, #state{listener=Listener}=State) ->
    {ok, Socket} = gen_tcp:accept(Listener),

    {ok, Pid} = ldb_static_peer_service_client:start_link(Socket),
    gen_tcp:controlling_process(Socket, Pid),

    accept_one(),

    {noreply, State};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
accept_one() ->
    gen_server:cast(?MODULE, accept).
