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

-define(CREATE_OVERLAY_TIME, 5000).
-define(EXPERIMENT_END_TIME, 30000).

%% ldb_dcos callbacks
-export([create_overlay/1,
         get_app_tasks/1]).

%% @docs
-spec create_overlay(atom()) -> ldb_node_id().
create_overlay(OverlayName) ->
    ldb_log:info("Will create the overlay ~p in ~p", [OverlayName, ?CREATE_OVERLAY_TIME]),
    timer:sleep(?CREATE_OVERLAY_TIME),

    %% Get tasks from marathon
    case get_app_tasks("ldbs") of
        {ok, Response} ->
            {value, {_, Tasks}} = lists:keysearch(<<"tasks">>, 1, Response),
            %% Process marathon reply
            {Names, NameToNodeInfo} = lists:foldl(
                fun(Task, {Names0, NameToNodeInfo0}) ->

                    %% Get task ip
                    {value, {_, Ip0}} = lists:keysearch(<<"host">>, 1, Task),
                    Ip = binary_to_list(Ip0),
                    {ok, IpAddress} = inet_parse:address(Ip),

                    %% Get task port
                    {value, {_, Ports}} = lists:keysearch(<<"ports">>, 1, Task),
                    [Port] = Ports,

                    %% Node name
                    Name = list_to_atom(
                        "ldb-" ++ integer_to_list(Port) ++ "@" ++ Ip
                    ),

                    Names1 = ordsets:add_element(Name, Names0),
                    NameToNodeInfo1 = orddict:store(Name, {Name, IpAddress, Port}, NameToNodeInfo0),
                    {Names1, NameToNodeInfo1}
                end,
                {[], []},
                Tasks
            ),

            {IdToName, MyId, _} = lists:foldl(
                fun(Name, {Acc0, MyId0, Counter0}) ->
                    Acc1 = orddict:store(Counter0, Name, Acc0),
                    MyId1 = case Name == node() of
                        true ->
                            Counter0;
                        false ->
                            MyId0
                    end,
                    Counter1 = Counter0 + 1,
                    {Acc1, MyId1, Counter1}
                end,
                {[], -1, 0},
                Names
            ),

            NodeNumber = ldb_config:node_number(),
            Overlay = ldb_overlay:get(OverlayName, NodeNumber),

            case length(Names) == NodeNumber of
                true ->
                    %% All are connected
                    ToConnectIds = orddict:fetch(MyId, Overlay),
                    connect(ToConnectIds, IdToName, NameToNodeInfo),
                    ok = schedule_simulation_end(MyId),
                    %% Return ldb node id
                    MyId;
                false ->
                    create_overlay(OverlayName)
            end;
        error ->
            create_overlay(OverlayName)
    end.

%% @doc
-spec get_app_tasks(string()) -> term().
get_app_tasks(App) ->
    Url = tasks_url(App),
    get_request(Url).

%% @private
connect([], _, _) -> ok;
connect([Id|Ids]=All, IdToName, NameToNodeInfo) ->
    Name = orddict:fetch(Id, IdToName),
    {Name, Ip, Port} = orddict:fetch(Name, NameToNodeInfo),
    RealNodeInfo = {Id, Ip, Port},
    case ldb_peer_service:join(RealNodeInfo) of
        ok ->
            connect(Ids, IdToName, NameToNodeInfo);
        Error ->
            ldb_log:info("Couldn't connect to ~p. Error ~p. Will try again in 5 seconds", [RealNodeInfo, Error]),
            timer:sleep(5000),
            connect(All, IdToName, NameToNodeInfo)
    end.

%% @private
-spec schedule_simulation_end(non_neg_integer()) -> ok.
schedule_simulation_end(MyId) ->
    case MyId == 0 of
        true ->
            spawn_link(
                fun() ->
                    check_dcos_experiment_end()
                end
            );
        false ->
            ok
    end,
    ok.

%% @private
-spec check_dcos_experiment_end() -> ok.
check_dcos_experiment_end() ->
    LogNumber = ldb_mongo:log_number(),
    NodeNumber = ldb_config:node_number(),

    case LogNumber == NodeNumber of
        true ->
            ldb_log:info("Simulation has ended. Will stop ldb", extended),
            stop_ldb();
        false ->
            ldb_log:info("Simulation has not ended. ~p of ~p", [LogNumber, NodeNumber], extended),
            timer:sleep(?EXPERIMENT_END_TIME),
            check_dcos_experiment_end()
    end.

%% @private
-spec request(get | delete, string()) -> term().
request(Method, Url) ->
    Headers = headers(),

    case httpc:request(Method, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            JSONBody = jsx:decode(Body),
            {ok, JSONBody};
        Error ->
            ldb_log:info("~p request with url ~p failed with ~p", [Method, Url, Error]),
            error
    end.

%% @private
-spec get_request(string()) -> term().
get_request(Url) ->
    request(get, Url).

%% @private
-spec delete_request(string()) -> term().
delete_request(Url) ->
    request(delete, Url).

%% @private
-spec stop_ldb() -> ok.
stop_ldb() ->
    Url = app_url("ldbs"),
    case delete_request(Url) of
        {ok, _} ->
            ok;
        error ->
            ldb_log:info("Stop ldb failed")
    end.

%% @private
-spec headers() -> [{string(), string()}].
headers() ->
    Token = ldb_config:dcos_token(),
    [{"Authorization", "token=" ++ Token}].

%% @private
-spec app_url(string()) -> string().
app_url(App) ->
    ldb_config:dcos_url() ++ "/service/marathon/v2/apps/" ++ App.

%% @private
-spec tasks_url(string()) -> string().
tasks_url(App) ->
    ldb_config:dcos_url() ++ "/service/marathon/v2/apps/" ++ App ++ "/tasks".
