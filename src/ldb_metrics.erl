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

-module(ldb_metrics).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_metrics callbacks
-export([start_link/0,
         get_all/0,
         record_transmission/1,
         record_memory/1,
         record_latency/2,
         record_processing/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-type transmission() :: maps:map(timestamp(), size_metric()).
-type memory() :: maps:map(timestamp(), two_size_metric()).
-type latency() :: maps:map(atom(), list(non_neg_integer())).
-type processing() :: non_neg_integer().

-record(state, {transmission :: transmission(),
                memory :: memory(),
                latency :: latency(),
                processing :: processing()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_all() -> {transmission(), memory(), latency(), processing()}.
get_all() ->
    gen_server:call(?MODULE, get_all, infinity).

-spec record_transmission(size_metric()) -> ok.
record_transmission({0, 0}) ->
    ok;
record_transmission(Size) ->
    gen_server:cast(?MODULE, {transmission, ldb_util:unix_timestamp(), Size}).

-spec record_memory(two_size_metric()) -> ok.
record_memory({{0, 0}, {0, 0}}) ->
    ok;
record_memory(TwoSize) ->
    gen_server:cast(?MODULE, {memory, ldb_util:unix_timestamp(), TwoSize}).

-spec record_latency(atom(), non_neg_integer()) -> ok.
record_latency(Type, MicroSeconds) ->
    gen_server:cast(?MODULE, {latency, Type, MicroSeconds}).

-spec record_processing(processing()) -> ok.
record_processing(0) ->
    ok;
record_processing(MicroSeconds) ->
    gen_server:cast(?MODULE, {processing, MicroSeconds}).

%% gen_server callbacks
init([]) ->
    lager:info("ldb_metrics initialized!"),
    {ok, #state{transmission=maps:new(),
                memory=maps:new(),
                latency=maps:new(),
                processing=0}}.

handle_call(get_all, _From, #state{transmission=Transmission,
                                   memory=Memory,
                                   latency=Latency,
                                   processing=Processing}=State) ->
    All = {Transmission, Memory, Latency, Processing},
    {reply, All, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({transmission, Timestamp, Size}, #state{transmission=Transmission0}=State) ->
    Transmission = maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:plus(V, Size) end,
        Size,
        Transmission0
    ),
    {noreply, State#state{transmission=Transmission}};

handle_cast({memory, Timestamp, TwoSize}, #state{memory=Memory0}=State) ->
    Memory = maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:two_plus(V, TwoSize) end,
        TwoSize,
        Memory0
    ),
    {noreply, State#state{memory=Memory}};

handle_cast({latency, Type, MicroSeconds}, #state{latency=Latency0}=State) ->
    Latency = maps:update_with(
        Type,
        fun(V) -> [MicroSeconds | V] end,
        [MicroSeconds],
        Latency0
    ),
    {noreply, State#state{latency=Latency}};

handle_cast({processing, MicroSeconds}, #state{processing=Processing}=State) ->
    ldb_util:qs(processing),
    {noreply, State#state{processing=Processing + MicroSeconds}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.
