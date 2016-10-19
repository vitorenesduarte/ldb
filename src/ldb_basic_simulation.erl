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

-behaviour(gen_server).

%% ldb_basic_simulation callbacks
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {local_events :: non_neg_integer()}).

-define(EVENT_NUMBER, 10).
-define(EVENT_INTERVAL, 5000).
-define(SIMULATION_END_INTERVAL, 10000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    ldb:create("SET", gset),
    schedule_event(),

    ldb_log:info("ldb_basic_simulation initialized!", extended),
    {ok, #state{local_events=0}}.

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(event, #state{local_events=LocalEvents0}=State) ->
    LocalEvents1 = LocalEvents0 + 1,
    Element = atom_to_list(node()) ++ integer_to_list(LocalEvents1),
    ldb:update("SET", {add, Element}),

    case LocalEvents1 == ?EVENT_NUMBER of
        true ->
            schedule_simulation_end();
        false ->
            schedule_event()
    end,

    {noreply, State#state{local_events=LocalEvents1}};

handle_info(simulation_end, State) ->
    {ok, ClientNumber} = application:get_env(?APP, client_number),
    {ok, Value} = ldb:query("SET"),
    Events = sets:size(Value),

    ldb_log:info("Events observed ~p | Node ~p", [Events, node()], extended),

    case Events == ClientNumber * ?EVENT_NUMBER of
        true ->
            ldb_log:info("All events have been observed", extended),
            ldb_instrumentation:convergence(),
            application:set_env(?APP, simulation_end, true);
        false ->
            schedule_simulation_end()
    end,

    {noreply, State};

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_event() ->
    timer:send_after(?EVENT_INTERVAL, event).

%% @private
schedule_simulation_end() ->
    timer:send_after(?SIMULATION_END_INTERVAL, simulation_end).
