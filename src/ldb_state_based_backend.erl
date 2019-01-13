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

-module(ldb_state_based_backend).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).

%% ldb_backend callbacks
-export([backend_state/0,
         bottom_entry/2,
         crdt/1,
         update/3,
         memory/1,
         message_maker/3,
         message_handler/4,
         message_size/1]).

-record(state, {id :: ldb_node_id()}).
-type st() :: #state{}.

%% crdt
-type stored() :: term().

-spec backend_state() -> st().
backend_state() ->
    Id = ldb_config:id(),
    #state{id=Id}.

-spec bottom_entry(term(), st()) -> stored().
bottom_entry(Bottom, _) ->
    Bottom.

-spec crdt(stored()) -> term().
crdt(CRDT) ->
    CRDT.

-spec update(stored(), operation(), st()) -> stored().
update({Type, _}=CRDT, Operation, #state{id=Id}) ->
    {ok, Result} = Type:mutate(Operation, Id, CRDT),
    Result.

-spec memory(stored()) -> size_metric().
memory(CRDT) ->
    {0, ldb_util:size(crdt, CRDT)}.

-spec message_maker(stored(), ldb_node_id(), st()) -> message().
message_maker(CRDT, _, _) ->
    {
        state,
        CRDT
    }.

-spec message_handler(message(), ldb_node_id(), stored(), st()) -> {stored(), nothing}.
message_handler({state, {Type, _}=RemoteCRDT}, _,
                LocalCRDT, _) ->
    {Type:merge(LocalCRDT, RemoteCRDT), nothing}.

-spec message_size(message()) -> size_metric().
message_size({state, CRDT}) ->
    {0, ldb_util:size(crdt, CRDT)}.
