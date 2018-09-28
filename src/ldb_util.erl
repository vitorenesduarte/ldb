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
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_util callbacks
-export([parse_membership/1,
         new_crdt/2,
         get_backend/0,
         atom_to_binary/1,
         binary_to_atom/1,
         unix_timestamp/0,
         size/2,
         plus/1,
         plus/2]).

-export([qs/1]).

%% @doc Parse membership from partisan.
-spec parse_membership(list(node_spec())) -> list(ldb_node_id()).
parse_membership(Membership) ->
    [Name || {Name, _, _} <- Membership, Name /= ldb_config:id()].

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
            ldb_scuttlebutt_backend
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
-spec unix_timestamp() -> timestamp().
unix_timestamp() ->
    erlang:system_time(second).

%% @doc
-spec size(crdt | digest | ack_map | delta_buffer | matrix | buffer, term()) ->
    {non_neg_integer(), non_neg_integer()}.
size(crdt, CRDT) ->
    state_type:crdt_size(CRDT);
size(digest, Digest) ->
    {state_type:digest_size(Digest), 0};
size(ack_map, AckMap) ->
    {orddict:size(AckMap), 0};
size(delta_buffer, DeltaBuffer) ->
    lists:foldl(
        fun({_Sequence, {_From, CRDT}}, Acc) ->
            plus([
                Acc,
                size(crdt, CRDT),
                %% +1 for the From and Sequence
                {1, 0}
            ])
        end,
        {0, 0},
        DeltaBuffer
    );
%% scuttlebutt
size(matrix, Matrix) ->
    %% matrix size is the sum of all vector sizes
    %% (ignoring the indexing key)
    Dots = maps:fold(
        fun(_, VV, Acc) -> Acc + maps:size(VV) end,
        0,
        Matrix
    ),
    {Dots, 0};
size(buffer, Buffer) ->
    lists:foldl(
        fun({_Dot, Delta}, Acc) ->
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
plus(L) ->
    lists:foldl(fun(E, Acc) -> plus(E, Acc) end, {0, 0}, L).
plus({A1, B1}, {A2, B2}) ->
    {A1 + A2, B1 + B2}.

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
qs(_ID) ->
    %{message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    %lager:info("MAILBOX - " ++ ID ++ " - REMAINING: ~p", [MessageQueueLen]).
    ok.
