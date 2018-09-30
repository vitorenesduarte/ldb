%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
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

%% @doc delta-buffer behaviour.

-module(ldb_dbuffer).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-export([should_send/5]).

-type buffer() :: term().

%% @doc Create new buffer.
-callback new(boolean(), function(), function()) -> buffer().

%% @doc Retrieve seq.
-callback seq(buffer()) -> sequence().

%% @doc Retrieve min_seq.
-callback min_seq(buffer()) -> sequence().

%% @doc Check if buffer is empty.
-callback is_empty(buffer()) -> boolean().

%% @doc Add to buffer.
-callback add_inflation(term(), ldb_node_id(), buffer()) -> buffer().

%% @doc Select from buffer.
-callback select(ldb_node_id(), sequence(), buffer()) -> [term()].

%% @doc Prune from buffer.
-callback prune(sequence(), buffer()) -> buffer().

%% @doc Size of buffer.
-callback size(buffer()) -> {non_neg_integer(), non_neg_integer()}.

%% @doc Pretty-print buffer.
-callback show(buffer()) -> term().

%% @doc Send if not seen (ack <= seq).
%%      If BP, only send if not the origin (from != to)
-spec should_send(ldb_node_id(), sequence(), ldb_node_id(), sequence(), boolean()) -> boolean().
should_send(From, Seq, To, LastAck, true) ->
    LastAck =< Seq andalso From =/= To;
should_send(_, Seq, _, LastAck, false) ->
    LastAck =< Seq.
