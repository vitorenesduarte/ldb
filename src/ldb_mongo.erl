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

-module(ldb_mongo).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-define(MONGO, mc_worker_api).
-define(DATABASE, <<"ldb">>).
-define(COLLECTION, <<"logs">>).

%% ldb_mongo callbacks
-export([log_number/0,
         push_logs/0,
         pull_logs/2]).

-spec log_number() -> non_neg_integer().
log_number() ->
    case get_connection() of
        {ok, Connection} ->
            EvaluationTimestamp = ldb_config:evaluation_timestamp(),
            ?MONGO:count(Connection,
                         ?COLLECTION,
                         {<<"timestamp">>, ldb_util:atom_to_binary(EvaluationTimestamp)});
        _ ->
            0
    end.

-spec push_logs() -> ok | error.
push_logs() ->
    case get_connection() of
        {ok, Connection} ->
            EvaluationTimestamp0 = ldb_config:evaluation_timestamp(),
            {Id0, Filename} = ldb_instrumentation:log_id_and_file(),
            Logs0 = get_logs(Filename),

            EvaluationTimestamp = ldb_util:atom_to_binary(EvaluationTimestamp0),
            Id = list_to_binary(Id0),
            Logs = list_to_binary(Logs0),

            ?MONGO:insert(Connection,
                          ?COLLECTION,
                          {<<"timestamp">>, EvaluationTimestamp,
                           <<"id">>, Id,
                           <<"logs">>, Logs}),
            ok;
        _ ->
            ldb_log:info("Couldn't push the logs to ldb-mongo. Will try again in 5 seconds"),
            timer:sleep(5),
            push_logs()
    end.

-spec pull_logs(string(), non_neg_integer()) -> term().
pull_logs(Host, Port) ->
    {ok, Connection} = ?MONGO:connect([{database, ?DATABASE},
                                       {host, Host},
                                       {port, Port}]),
    Cursor = ?MONGO:find(Connection, ?COLLECTION, #{}, #{skip =>0, batchsize => 0}),
    Logs = mc_cursor:rest(Cursor),
    mc_cursor:close(Cursor),
    Logs.

%% @private
get_connection() ->
    case ldb_dcos:get_app_tasks("ldb-mongo") of
        {ok, Response} ->
            {value, {_, [Task]}} = lists:keysearch(<<"tasks">>, 1, Response),
            {value, {_, Host0}} = lists:keysearch(<<"host">>, 1, Task),
            Host = binary_to_list(Host0),
            {value, {_, [Port]}} = lists:keysearch(<<"ports">>, 1, Task),

            {ok, Connection} = ?MONGO:connect([{database, ?DATABASE},
                                               {host, Host},
                                               {port, Port}]),
            {ok, Connection};
        error ->
            ldb_log:info("Cannot contact Marathon!"),
            error
    end.

%% @private
get_logs(Filename) ->
    Lines = ldb_util:read_lines(Filename),
    lists:foldl(
        fun(Line, Acc) ->
            Acc ++ Line
        end,
        "",
        Lines
    ).
