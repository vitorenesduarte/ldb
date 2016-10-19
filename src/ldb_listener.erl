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

-module(ldb_listener).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_listener callbacks
-export([start_link/0,
         handle_message/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec handle_message(term()) -> ok.
handle_message(Message) ->
    gen_server:cast(?MODULE, {handle_message, Message}).

%% gen_server callbacks
init([]) ->
    ldb_log:info("ldb_listener initialized!", extended),
    {ok, #state{}}.

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({handle_message, Message}, State) ->
    MessageHandler = ldb_backend:message_handler(Message),
    MessageHandler(Message),
    {noreply, State};

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
