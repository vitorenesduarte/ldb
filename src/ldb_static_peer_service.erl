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
         node_spec/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tref :: timer:tref()}).

-define(SYNC_INTERVAL, 5000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec members() -> {ok, [node()]}.
members() ->
    %% @todo
    {ok, []}.

-spec join(specs()) -> ok | error().
join(_) ->
    %% @todo
    ok.

-spec forward_message(node(), pid(), message()) -> ok.
forward_message(_, _, _) ->
    %% @todo
    ok.

-spec node_spec() -> {ok, specs()}.
node_spec() ->
    %% @todo
    {name, {127, 0, 0, 1}, 8765}.

%% gen_server callbacks
init([]) ->
    {ok, TRef} = schedule_sync(),

    lager:info("ldb_static_peer_service initialized!"),
    {ok, #state{tref=TRef}}.

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(sync, State) ->

    lager:info("Node ~p | Members ~p", [node(), ldb_peer_service:members()]),

    {ok, TRef} = schedule_sync(),
    {noreply, State#state{tref=TRef}};

handle_info(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_sync() ->
    timer:send_after(?SYNC_INTERVAL, sync).
