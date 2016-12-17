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

-module(ldb_log).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_log callbacks
-export([info/1,
         info/2,
         info/3,
         warning/1,
         warning/2]).

-spec info(string()) -> ok.
info(S) ->
    handle(lager:info(S)).

-spec info(string(), extended | list()) -> ok.
info(S, extended) ->
    info(S, [], extended);

info(S, Args) ->
    handle(lager:info(S, Args)).

-spec info(string(), list(), extended) -> ok.
info(S, Args, extended) ->
    case ldb_config:get(extended_logging, false) of
        true ->
            handle(lager:info(S, Args));
        false ->
            ok
    end.

-spec warning(string()) -> ok.
warning(S) ->
    handle(lager:warning(S)).

-spec warning(string(), list()) -> ok.
warning(S, Args) ->
    handle(lager:warning(S, Args)).

%% @private
handle(_LagerResult) ->
    ok.
