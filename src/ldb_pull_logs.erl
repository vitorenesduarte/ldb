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

-module(ldb_pull_logs).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_pull_logs callbacks
-export([go/2]).

%% @doc Pulls logs from the running ldb-mongo instance on DCOS.
-spec go(string(), non_neg_integer()) -> ok.
go(Host, Port) ->
    Logs = ldb_mongo:pull_logs(Host, Port),
    lists:foldl(
        fun(Log, NodeNumber0) ->
            SimulationId0 = maps:get(<<"id">>, Log),
            NodeLogs0 = maps:get(<<"logs">>, Log),

            SimulationId = binary_to_list(SimulationId0),
            NodeLogs = binary_to_list(NodeLogs0),

            LogDir = log_dir(SimulationId),
            LogFile = LogDir
                   ++ "node_"
                   ++ integer_to_list(NodeNumber0)
                   ++ ".csv",
            filelib:ensure_dir(LogDir),

            ok = file:write_file(LogFile, NodeLogs, [write]),

            NodeNumber0 + 1
        end,
        0,
        Logs
    ),
    ok.

%% @private
root_eval_dir() ->
    code:priv_dir(?APP) ++ "/evaluation".

%% @private
root_log_dir() ->
    root_eval_dir() ++ "/logs".

%% @private
log_dir(SimulationId) ->
    root_log_dir() ++ "/" ++ SimulationId ++ "/".
