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

-module(ldb_backend).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% @doc Returns backend config.
-callback backend_state() -> backend_state().

%% @doc Returns a backend entry given a bottom CRDT value.
-callback bottom_entry(term(), backend_state()) -> backend_stored().

%% @doc Return the CRDT value, to be used when reading.
-callback crdt(backend_stored()) -> term().

%% @doc Update a stored value applying a given `operation()'.
-callback update(backend_stored(), operation(), backend_state()) -> backend_stored().

%% @doc Returns memory consumption.
-callback memory(backend_stored()) -> two_size_metric().

%% @doc Returns message to be sent.
%%      - if `Message == nothing', no message is sent.
-callback message_maker(backend_stored(), ldb_node_id(), backend_state()) ->
    message() | nothing.

%% @doc Returns a function that handles the message received.
-callback message_handler(message(), ldb_node_id(), backend_stored(), backend_state()) ->
    {backend_stored(), message() | nothing}.

%% @doc Return the size of a given message.
-callback message_size(message()) -> size_metric().
