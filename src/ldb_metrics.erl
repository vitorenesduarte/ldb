%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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

-module(ldb_metrics).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_metrics callbacks
-export([start_link/0,
         get_time_series/0,
         get_latency/0,
         record_message/2,
         record_latency/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type metric_type() :: transmission | memory.
-type metric() :: term().
-type time_series() :: list({timestamp(), metric_type(), metric()}).

-type latency_type() :: local | term().
-type latency() :: list({latency_type(), list(integer())}).

-record(state, {message_type_to_size :: orddict:orddict(),
                time_series :: time_series(),
                latency_type_to_latency :: orddict:orddict()}).

-define(TIME_SERIES_INTERVAL, 1000). %% 1 second.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_time_series() -> time_series().
get_time_series() ->
    gen_server:call(?MODULE, get_time_series, infinity).

-spec get_latency() -> latency().
get_latency() ->
    gen_server:call(?MODULE, get_latency, infinity).

-spec record_message(term(), message()) -> ok.
record_message(MessageType, Message) ->
    Metrics = message_metrics(Message),
    gen_server:cast(?MODULE, {message, MessageType, Metrics}).

%% @doc Record latency of:
%%          - `local': creating a message locally
%%          - `MessageType': applying a message (with type `MessageType') remotely
-spec record_latency(latency_type(), integer()) -> ok.
record_latency(Type, MicroSeconds) ->
    gen_server:cast(?MODULE, {latency, Type, MicroSeconds}).

%% gen_server callbacks
init([]) ->
    schedule_time_series(),
    ?LOG("ldb_metrics initialized!"),
    {ok, #state{message_type_to_size=orddict:new(),
                latency_type_to_latency=orddict:new(),
                time_series=[]}}.

handle_call(get_time_series, _From,
            #state{time_series=TimeSeries}=State) ->
    {reply, TimeSeries, State};

handle_call(get_latency, _From,
            #state{latency_type_to_latency=Map}=State) ->
    {reply, Map, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({message, MessageType, Metrics},
            #state{message_type_to_size=Map0}=State) ->
    Size = orddict:fetch(size, Metrics),
    Current = orddict_ext:fetch(MessageType, Map0, 0),
    Map1 = orddict:store(MessageType, Current + Size, Map0),
    {noreply, State#state{message_type_to_size=Map1}};

handle_cast({latency, Type, MicroSeconds},
            #state{latency_type_to_latency=Map0}=State) ->
    Map1 = orddict:append(Type, MicroSeconds, Map0),
    {noreply, State#state{latency_type_to_latency=Map1}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(time_series, #state{message_type_to_size=MessageMap,
                                time_series=TimeSeries0}=State) ->
    Timestamp = ldb_util:unix_timestamp(),

    % transmission metrics
    TimeSeries1 = case orddict:is_empty(MessageMap) of
        true ->
            TimeSeries0;
        false ->
            TMetric = {Timestamp, transmission, MessageMap},
            lists:append(TimeSeries0, [TMetric])
    end,

    % memory metrics
    MMetric = {Timestamp, memory, ldb_backend:memory()},
    TimeSeries2 = lists:append(TimeSeries1, [MMetric]),

    schedule_time_series(),

    {noreply, State#state{time_series=TimeSeries2}};

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_time_series() ->
    timer:send_after(?TIME_SERIES_INTERVAL, time_series).

%% @private
message_metrics(Message) ->
    Size = ldb_util:term_size(Message),
    [{size, Size}].
