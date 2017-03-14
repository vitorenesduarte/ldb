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
-export([get_type/1,
         get_backend/0,
         atom_to_binary/1,
         binary_to_atom/1,
         unix_timestamp/0,
         term_size/1]).

%% @doc Returns the actual type in types repository
%%      (https://github.com/lasp-lang/types)
-spec get_type(atom()) -> atom().
get_type(Type) ->
    Map = types_map(),
    {State, Op} = orddict:fetch(Type, Map),
    case ldb_config:get(ldb_mode, ?DEFAULT_MODE) of
        state_based ->
            State;
        delta_based ->
            State;
        pure_op_based ->
            Op
    end.

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
-spec term_size(term()) -> non_neg_integer().
term_size(T) ->
    byte_size(term_to_binary(T)).

%% @private
types_map() ->
    Map0 = orddict:new(),
    Map1 = orddict:store(gcounter, {state_gcounter, pure_gcounter}, Map0),
    Map2 = orddict:store(gset, {state_gset, pure_gset}, Map1),
    Map3 = orddict:store(mvmap, {state_mvmap, undefined}, Map2),
    Map4 = orddict:store(mvregister, {state_mvregister, pure_mvregister}, Map3),
    Map5 = orddict:store(awset, {state_awset, pure_awset}, Map4),
    Map6 = orddict:store(pncounter, {state_pncounter, pure_pncounter}, Map5),
    Map7 = orddict:store(lwwregister, {state_lwwregister, undefined}, Map6),
    Map7.
