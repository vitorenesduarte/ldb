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

-module(ldb_basic_simulation).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_basic_simulation callbacks
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(EVENT_INTERVAL, 3000).
-define(MAX_VALUE, 10).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    ldb:create("counter", gcounter),
    schedule_event(),

    lager:info("ldb_basic_simulation initialized!"),
    {ok, #state{}}.

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(event, State) ->
    ldb:update("counter", increment),
    Value = ldb:query("counter"),

    lager:info("Node ~p: I did an increment and the value now is ~p", [node(), Value]),

    case Value < ?MAX_VALUE of
        true ->
            schedule_event();
        false ->
            lager:info("All increments have been observed"),
            application:set_env(?APP, simulation_end, true)
    end,

    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_event() ->
    timer:send_after(?EVENT_INTERVAL, event).

