%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
         wait_until/3,
         timestamp/0,
         atom_to_binary/1,
         binary_to_atom/1,
         read_lines/1]).

%% @doc Returns the actual type in types repository
%%      (https://github.com/lasp-lang/types)
-spec get_type(atom()) -> atom().
get_type(Type) ->
    Map = types_map(),
    {State, Op} = orddict:fetch(Type, Map),
    case ldb_config:mode() of
        state_based ->
            State;
        delta_based ->
            State;
        pure_op_based ->
            Op
    end.

%% @todo add spec
%% @doc Wait until `Fun' returns true or `Retry' reaches 0.
%%      The sleep time between retries is `Delay'.
wait_until(_Fun, 0, _Delay) -> fail;
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    case Fun() of
        true ->
            ok;
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry - 1, Delay)
    end.

%% @doc Returns unix timestamp
-spec timestamp() -> non_neg_integer().
timestamp() ->
    {Mega, Sec, _Micro} = erlang:timestamp(),
    Mega * 1000000 + Sec.

%% @doc
-spec atom_to_binary(atom()) -> binary().
atom_to_binary(Atom) ->
    erlang:atom_to_binary(Atom, utf8).

%% @doc
-spec binary_to_atom(binary()) -> atom().
binary_to_atom(Binary) ->
    erlang:binary_to_atom(Binary, utf8).

%% @doc
-spec read_lines(string()) -> [string()].
read_lines(FilePath) ->
    {ok, FileDescriptor} = file:open(FilePath, [read]),
    Lines = get_lines(FilePath, FileDescriptor),
    Lines.

%% @private
get_lines(FilePath, FileDescriptor) ->
    case io:get_line(FileDescriptor, '') of
        eof ->
            [];
        {error, Error} ->
            ldb_log:warning("Error while reading line from file ~p. Error: ~p", [FilePath, Error]),
            [];
        Line ->
            [Line | get_lines(FilePath, FileDescriptor)]
    end.

%% @private
types_map() ->
    Map0 = orddict:new(),
    Map1 = orddict:store(gcounter, {state_gcounter, pure_gcounter}, Map0),
    Map2 = orddict:store(gset, {state_gset, pure_gset}, Map1),
    Map3 = orddict:store(mvmap, {state_mvmap, undefined}, Map2),
    Map3.
