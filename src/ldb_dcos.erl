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

-module(ldb_dcos).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_dcos callbacks
-export([push_logs/0]).

%% @doc
push_logs() ->
    mongo(),
    ldbs().

%% @private
mongo() ->
    Url = task_url("ldb-mongo"),
    {ok, R} = get_request(Url),
    D = jiffy:decode(R),
    lager:info("~n~n~nDECODE~n~p~n~n", [D]).

%% @private
ldbs() ->
    Url = task_url("ldbs"),
    {ok, R} = get_request(Url),
    D = jiffy:decode(R),
    lager:info("~n~n~nDECODE~n~p~n~n", [D]).

%% @private
get_request(Url) ->
    Headers = headers(),

    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            lager:info("~n~n~nREPLY~n~p~n~n", [Body]),
            {ok, Body};
        Error ->
            ldb_log:info("Get request with url ~p failed with ~p", [Url, Error], extended),
            error
    end.

%% @private
headers() ->
    [{"Authorization", "token=" ++ token()}].

%% @private
dcos() ->
    os:getenv("DCOS", "undefined").

%% @private
token() ->
    os:getenv("TOKEN", "undefined").

%% @private
task_url(Task) ->
    dcos() ++ "/service/marathon/v2/apps/" ++ Task ++ "/tasks".
