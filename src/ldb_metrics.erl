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
         record_message/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type timestamp() :: non_neg_integer().
-type metric_type() :: message.
-type metric() :: term().
-type time_series() :: list({timestamp(), metric_type(), metric()}).

-record(state, {message_type_to_size :: orddict:orddict(),
                time_series :: time_series()}).

-define(TIME_SERIES_INTERVAL, 1000). %% 1 second.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_time_series() -> time_series().
get_time_series() ->
    gen_server:call(?MODULE, get_time_series, infinity).

-spec record_message(term(), message()) -> ok.
record_message(Type, Message) ->
    Metrics = message_metrics(Message),
    gen_server:cast(?MODULE, {message, Type, Metrics}).

%% gen_server callbacks
init([]) ->
    schedule_time_series(),
    ?LOG("ldb_metrics initialized!"),
    {ok, #state{message_type_to_size=orddict:new(),
                time_series=[]}}.

handle_call(get_time_series, _From,
            #state{time_series=TimeSeries}=State) ->
    {reply, TimeSeries, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({message, Type, Metrics},
            #state{message_type_to_size=Map0}=State) ->
    Size = orddict:fetch(size, Metrics),
    Current = orddict_ext:fetch(Type, Map0, 0),
    Map1 = orddict:store(Type, Current + Size, Map0),
    {noreply, State#state{message_type_to_size=Map1}}.

handle_info(time_series, #state{message_type_to_size=MessageMap,
                                time_series=TimeSeries0}=State) ->
    TimeSeries1 = case orddict:is_empty(MessageMap) of
        true ->
            TimeSeries0;
        false ->
            Timestamp = unix_timestamp(),
            MetricType = message,
            Metric = {Timestamp, MetricType, MessageMap},
            lists:append(TimeSeries0, [Metric])
    end,
    {noreply, State#state{time_series=TimeSeries1}};

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
    Size = term_size(Message),
    [{size, Size}].

%% @private
term_size(T) ->
    byte_size(term_to_binary(T)).

%% @private
unix_timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.
