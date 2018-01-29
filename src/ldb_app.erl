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

-module(ldb_app).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-behaviour(application).

-export([start/2,
         stop/1,
         happened_before/2]).

%% @doc Initialize the application.
start(_StartType, _StartArgs) ->
    application:set_env(types, happened_before_mod, ?MODULE),
    application:set_env(types, happened_before_fun, happened_before),
    case ldb_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.

%% @doc Stop the application.
stop(_State) ->
    ok.


happened_before(A, B) ->
    case vclock:compare(A, B) of
        lt -> true;
        _ -> false
    end.
