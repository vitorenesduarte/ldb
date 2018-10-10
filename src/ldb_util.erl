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

-module(ldb_util).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_util callbacks
-export([new_crdt/2,
         get_backend/0,
         atom_to_binary/1,
         binary_to_atom/1,
         integer_to_atom/1,
         unix_timestamp/0,
         size/2,
         plus/1,
         plus/2,
         two_plus/2]).

-export([qs/1]).

%% @doc Creates a bottom CRDT from a type
%%      or from an existing state-based CRDT.
new_crdt(type, CType) ->
    {Type, Args} = extract_args(CType),
    case Args of
        [] ->
            Type:new();
        _ ->
            Type:new(Args)
    end;
new_crdt(state, CRDT) ->
    %% defined in lasp-lang/types.
    state_type:new(CRDT).

%% @doc Returns the proper backend.
-spec get_backend() -> atom().
get_backend() ->
    case ldb_config:get(ldb_mode, ?DEFAULT_MODE) of
        state_based ->
            ldb_state_based_backend;
        delta_based ->
            ldb_delta_based_backend;
        scuttlebutt ->
            ldb_scuttlebutt_backend;
        vanilla_scuttlebutt ->
            ldb_vanilla_scuttlebutt_backend
    end.

%% @doc
-spec atom_to_binary(atom()) -> binary().
atom_to_binary(Atom) ->
    erlang:atom_to_binary(Atom, utf8).

%% @doc
-spec binary_to_atom(binary()) -> atom().
binary_to_atom(Binary) ->
    erlang:binary_to_atom(Binary, utf8).

%% @doc
-spec integer_to_atom(integer()) -> atom().
integer_to_atom(Integer) ->
    list_to_atom(integer_to_list(Integer)).

%% @doc
-spec unix_timestamp() -> timestamp().
unix_timestamp() ->
    erlang:system_time(second).

%% @doc
-spec size(crdt | ack_map | vector | matrix | dotted_buffer, term()) ->
    {non_neg_integer(), non_neg_integer()}.
size(crdt, CRDT) ->
    state_type:crdt_size(CRDT);
size(ack_map, AckMap) ->
    {maps:size(AckMap), 0};
%% scuttlebutt
size(vector, VV) ->
    {vclock:size(VV), 0};
size(matrix, Matrix) ->
    %% matrix size is the sum of all vector sizes
    %% plus the number of entries in the matrix
    Dots = maps:fold(
        fun(_, VV, Acc) -> Acc + 1 + vclock:size(VV) end,
        0,
        Matrix
    ),
    {Dots, 0};
size(dotted_buffer, Buffer) ->
    maps:fold(
        fun(_Dot, Delta, Acc) ->
            plus([
                Acc,
                size(crdt, Delta),
                %% +1 for the dot
                {1, 0}
            ])
        end,
        {0, 0},
        Buffer
    ).

%% @doc sum
-spec plus([size_metric()]) -> size_metric().
plus(L) ->
    lists:foldl(fun(E, Acc) -> plus(E, Acc) end, {0, 0}, L).

%% @doc
-spec plus(size_metric(), size_metric()) -> size_metric().
plus({A1, B1}, {A2, B2}) ->
    {A1 + A2, B1 + B2}.

%% @doc
-spec two_plus(two_size_metric(), two_size_metric()) -> two_size_metric().
two_plus({A1, B1}, {A2, B2}) ->
    {plus(A1, A2), plus(B1, B2)}.

%% @private
extract_args({Type, Args}) ->
    {get_type(Type), get_type(Args)};
extract_args(Type) ->
    {get_type(Type), []}.

%% @private
get_type({A, B}) ->
    {get_type(A), get_type(B)};
get_type([]) ->
    [];
get_type([H|T]) ->
    [get_type(H) | get_type(T)];
get_type(Type) ->
    list_to_atom("state_" ++ atom_to_list(Type)).

%% @doc Log Process queue length.
qs(ID) ->
    {message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    lager:info("MAILBOX ~p REMAINING: ~p", [ID, MessageQueueLen]).
