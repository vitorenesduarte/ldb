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

%% ldb_metrics callbacks
-export([start/0,
         get_all/0,
         record_transmission/1,
         record_memory/1,
         record_latency/2,
         record_processing/1]).

-type transmission() :: maps:map(timestamp(), size_metric()).
-type memory() :: maps:map(timestamp(), two_size_metric()).
-type latency() :: maps:map(atom(), list(non_neg_integer())).
-type processing() :: non_neg_integer().

-spec start() -> ok.
start() ->
    %% create transmission keeper
    {ok, _} = ldb_metrics_keeper:start_link(
        transmission_keeper,
        maps:new(),
        fun update_transmission/2
    ),
    %% create memory keeper
    {ok, _} = ldb_metrics_keeper:start_link(
        memory_keeper,
        maps:new(),
        fun update_memory/2
    ),
    %% create latency keeper
    {ok, _} = ldb_metrics_keeper:start_link(
        latency_keeper,
        maps:new(),
        fun update_latency/2
    ),
    %% create processing keeper
    {ok, _} = ldb_metrics_keeper:start_link(
        processing_keeper,
        0,
        fun update_processing/2
    ),
    ok.

-spec get_all() -> {transmission(), memory(), latency(), processing()}.
get_all() ->
    Transmission = ldb_metrics_keeper:get(transmission_keeper),
    Memory = ldb_metrics_keeper:get(memory_keeper),
    Latency = ldb_metrics_keeper:get(latency_keeper),
    Processing = ldb_metrics_keeper:get(processing_keeper),
    {Transmission, Memory, Latency, Processing}.

-spec record_transmission(size_metric()) -> ok.
record_transmission({0, 0}) ->
    ok;
record_transmission(Size) ->
    Args = {ldb_util:unix_timestamp(), Size},
    ldb_metrics_keeper:record(transmission_keeper, Args).

-spec record_memory(two_size_metric()) -> ok.
record_memory({{0, 0}, {0, 0}}) ->
    ok;
record_memory(TwoSize) ->
    Args = {ldb_util:unix_timestamp(), TwoSize},
    ldb_metrics_keeper:record(memory_keeper, Args).

-spec record_latency(atom(), non_neg_integer()) -> ok.
record_latency(Type, MicroSeconds) ->
    Args = {Type, MicroSeconds},
    ldb_metrics_keeper:record(latency_keeper, Args).

-spec record_processing(processing()) -> ok.
record_processing(0) ->
    ok;
record_processing(MicroSeconds) ->
    ldb_metrics_keeper:record(processing_keeper, MicroSeconds).

update_transmission({Timestamp, Size}, Transmission0) ->
    maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:plus(V, Size) end,
        Size,
        Transmission0
    ).

update_memory({Timestamp, TwoSize}, Memory0) ->
    maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:two_plus(V, TwoSize) end,
        TwoSize,
        Memory0
    ).

update_latency({Type, MicroSeconds}, Latency0) ->
    maps:update_with(
        Type,
        fun(V) -> [MicroSeconds | V] end,
        [MicroSeconds],
        Latency0
    ).

update_processing(MicroSeconds, Processing) ->
    Processing + MicroSeconds.

