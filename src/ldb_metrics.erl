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
-export([new/0,
         merge_all/1,
         record_transmission/2,
         record_memory/2,
         record_latency/3,
         record_processing/2]).

-type transmission() :: maps:map(timestamp(), size_metric()).
-type memory() :: maps:map(timestamp(), two_size_metric()).
-type latency() :: maps:map(atom(), list(non_neg_integer())).
-type processing() :: non_neg_integer().

-record(state, {transmission :: transmission(),
                memory :: memory(),
                latency :: latency(),
                processing :: processing()}).
-type st() :: #state{}.

-spec new() -> st().
new() ->
    #state{transmission=maps:new(),
           memory=maps:new(),
           latency=maps:new(),
           processing=0}.

-spec merge_all(list(st())) -> {transmission(), memory(), latency(), processing()}.
merge_all([A, B | T]) ->
    #state{transmission=TransmissionA,
           memory=MemoryA,
           latency=LatencyA,
           processing=ProcessingA} = A,
    #state{transmission=TransmissionB,
           memory=MemoryB,
           latency=LatencyB,
           processing=ProcessingB} = B,
    Transmission = maps_ext:merge_all(
        fun(_, VA, VB) -> ldb_util:plus(VA, VB) end,
        TransmissionA,
        TransmissionB
    ),
    Memory = maps_ext:merge_all(
        fun(_, VA, VB) -> ldb_util:two_plus(VA, VB) end,
        MemoryA,
        MemoryB
    ),
    Latency = maps_ext:merge_all(
        fun(_, VA, VB) -> VA ++ VB end,
        LatencyA,
        LatencyB
    ),
    Processing = ProcessingA + ProcessingB,
    H = #state{transmission=Transmission,
               memory=Memory,
               latency=Latency,
               processing=Processing},
    merge_all([H | T]);
merge_all([#state{transmission=Transmission,
                  memory=Memory,
                  latency=Latency,
                  processing=Processing}]) ->
    {Transmission, Memory, Latency, Processing}.

-spec record_transmission(size_metric(), st()) -> st().
record_transmission({0, 0}, State) ->
    State;
record_transmission(Size, #state{transmission=Transmission0}=State) ->
    Transmission = update_transmission(ldb_util:unix_timestamp(), Size, Transmission0),
    State#state{transmission=Transmission}.

-spec record_memory(two_size_metric(), st()) -> st().
record_memory({{0, 0}, {0, 0}}, State) ->
    State;
record_memory(TwoSize, #state{memory=Memory0}=State) ->
    Memory = update_memory(ldb_util:unix_timestamp(), TwoSize, Memory0),
    State#state{memory=Memory}.

-spec record_latency(atom(), non_neg_integer(), st()) -> st().
record_latency(Type, MicroSeconds, #state{latency=Latency0}=State) ->
    Latency = update_latency(Type, MicroSeconds, Latency0),
    State#state{latency=Latency}.

-spec record_processing(processing(), st()) -> st().
record_processing(MicroSeconds, #state{processing=Processing0}=State) ->
    State#state{processing=Processing0 + MicroSeconds}.

update_transmission(Timestamp, Size, Transmission0) ->
    maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:plus(V, Size) end,
        Size,
        Transmission0
    ).

update_memory(Timestamp, TwoSize, Memory0) ->
    maps:update_with(
        Timestamp,
        fun(V) -> ldb_util:two_plus(V, TwoSize) end,
        TwoSize,
        Memory0
    ).

update_latency(Type, MicroSeconds, Latency0) ->
    maps:update_with(
        Type,
        fun(V) -> [MicroSeconds | V] end,
        [MicroSeconds],
        Latency0
    ).
