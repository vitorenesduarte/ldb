%%
%% Copyright (c) 2016-2018 Vitor Enes.  All Rights Reserved.
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

-module(ldb_metrics_keeper).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_metrics_keeper callbacks
-export([start_link/3,
         get/1,
         record/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-type stored() :: term().
-type args() :: term().
-record(state, {stored :: stored(),
                update_fun :: function()}).

-spec start_link(atom(), stored(), function()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, Initial, UpdateFun) ->
    gen_server:start_link({local, Name}, ?MODULE, [Initial, UpdateFun], []).

-spec get(atom()) -> stored().
get(Name) ->
    gen_server:call(Name, get, infinity).

-spec record(atom(), args()) -> ok.
record(Name, Args) ->
    gen_server:cast(Name, {record, Args}).

%% gen_server callbacks
init([Initial, UpdateFun]) ->
    lager:info("ldb_metrics_keeper initialized!"),
    {ok, #state{stored=Initial,
                update_fun=UpdateFun}}.

handle_call(get, _From, #state{stored=Stored}=State) ->
    {reply, Stored, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({record, Args}, #state{stored=Stored0,
                                   update_fun=UpdateFun}=State) ->
    ldb_util:qs(record),
    Stored = UpdateFun(Args, Stored0),
    {noreply, State#state{stored=Stored}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.
