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

%% ldb_static_peer_service_server callbacks
-export([get_type/1,
         wait_until/3]).

%% @doc Returns the actual type in types repository
%%      (https://github.com/lasp-lang/types)
-spec get_type(atom()) -> atom().
get_type(Type) ->
    Map = [{gcounter, {state_gcounter, pure_gcounter}},
           {gset, {state_gset, pure_gset}}],
    {State, _Op} = orddict:fetch(Type, Map),
    case ldb_config:mode() of
        state_based ->
            State;
        delta_based ->
            State
    end.

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
