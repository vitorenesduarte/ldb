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

-module(ldb_util).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_util callbacks
-export([new_crdt/2,
         get_backend/0,
         atom_to_binary/1,
         binary_to_atom/1,
         unix_timestamp/0,
         size/2]).

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
        pure_op_based ->
            ldb_pure_op_based_backend
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
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.

%% @doc
-spec size(term | crdt | delta_buffer, term()) -> non_neg_integer().
size(term, T) ->
    erts_debug:flat_size(T);
size(crdt, CRDT) ->
    state_type:crdt_size(CRDT);
size(delta_buffer, DeltaBuffer) ->
    lists:foldl(
        fun({Sequence, {From, CRDT}}, Acc) ->
            Acc + size(crdt, CRDT) + size(term, {Sequence, From})
        end,
        0,
        DeltaBuffer
    ).

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
    {State, Op} = orddict:fetch(Type, types_map()),
    case ldb_config:get(ldb_mode, ?DEFAULT_MODE) of
        state_based ->
            State;
        delta_based ->
            State;
        pure_op_based ->
            Op
    end.

%% @private
types_map() ->
    Types = [{awset, {state_awset, pure_awset}},
             {boolean, {state_boolean, undefined}},
             {dwflag, {state_dwflag, pure_dwflag}},
             {ewflag, {state_ewflag, pure_ewflag}},
             {gcounter, {state_gcounter, pure_gcounter}},
             {gmap, {state_gmap, undefined}},
             {gset, {state_gset, pure_gset}},
             {lexcounter, {state_lexcounter, undefined}},
             {lwwregister, {state_lwwregister, undefined}},
             {max_int, {state_max_int, undefined}},
             {mvregister, {state_mvregister, pure_mvregister}},
             {mvmap, {state_mvmap, undefined}},
             {ormap, {state_ormap, undefined}},
             {pair, {state_pair, undefined}},
             {pncounter, {state_pncounter, undefined}},
             {twopset, {state_twopset, pure_twopset}}],
    orddict:from_list(Types).

%% @doc Log Process queue length.
qs(ID) ->
    %{message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    %lager:info("MAILBOX - " ++ ID ++ " - REMAINING: ~p", [MessageQueueLen]).
    ok.
