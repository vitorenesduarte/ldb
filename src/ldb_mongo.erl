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

-define(RETRY_TIME, 5000).
-define(MONGO, mc_worker_api).
-define(DATABASE, <<"ldb">>).
-define(COLLECTION, <<"logs">>).

-behaviour(gen_server).

%% ldb_store callbacks
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {connection}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
    case ldb_dcos:get_task_info("ldb-mongo") of
        {ok, Response} ->
            lager:info("RESPONSE ~p", [Response]),
            {value, {_, [Task]}} = lists:keysearch(<<"tasks">>, 1, Response),
            lager:info("TASK ~p", [Task]),
            {value, {_, Host0}} = lists:keysearch(<<"host">>, 1, Task),
            Host = binary_to_list(Host0),
            {value, {_, [Port]}} = lists:keysearch(<<"ports">>, 1, Task),

            {ok, Connection} = ?MONGO:connect([{database, ?DATABASE},
                                               {host, Host},
                                               {port, Port}]),

            ?MONGO:insert(Connection, ?COLLECTION,
                          [{<<"id">>, <<"task-id-123456">>}]),

            Count = ?MONGO:count(Connection, ?COLLECTION,
                                 {<<"id">>, <<"task-id-123456">>}),

            lager:info("COUNTCOUNT ~p", [Count]),

            ldb_log:info("ldb_mongo initialized!", extended),
            {ok, #state{connection=Connection}};
        error ->
            timer:sleep(?RETRY_TIME),
            init([])
    end.

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
