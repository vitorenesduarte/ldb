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

-define(WAIT_TIME, 0).
-define(RETRY_TIME, 5000).

%% ldb_dcos callbacks
-export([create_overlay/0]).

%% @docs
create_overlay() ->
    ldb_log:info("Will create the overlay in ~p", [?WAIT_TIME]),
    timer:sleep(?WAIT_TIME),
    ldb_log:info("Will create the overlay"),

    %% Get tasks from marathon
    Url = task_url("ldbs"),
    {ok, Response} = get_request(Url),
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

    Overlay = line(),

    NodeNumber = ldb_config:node_number(),
    case length(Names) == NodeNumber of
        true ->
            %% All are connected
            ToConnectIds = orddict:fetch(MyId, Overlay),
            connect(ToConnectIds, IdToName, NameToNodeInfo);
        false ->
            timer:sleep(?RETRY_TIME),
            create_overlay()
    end.

%% @private
connect([], _, _) -> ok;
connect([Id|Ids]=All, IdToName, NameToNodeInfo) ->
    Name = orddict:fetch(Id, IdToName),
    NodeInfo = orddict:fetch(Name, NameToNodeInfo),
    case ldb_peer_service:join(NodeInfo) of
        ok ->
            connect(Ids, IdToName, NameToNodeInfo);
        Error ->
            ldb_log:info("Couldn't connect to ~p. Error ~p. Will try again in 5 seconds", [NodeInfo, Error]),
            connect(All, IdToName, NameToNodeInfo)
    end.

%% @private
get_request(Url) ->
    Headers = headers(),

    case httpc:request(get, {Url, Headers}, [], [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            JSONBody = jsx:decode(Body),
            {ok, JSONBody};
        Error ->
            ldb_log:info("Get request with url ~p failed with ~p", [Url, Error]),
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

%% @private
line() ->
    [
     {0, [1]},
     {1, [2]},
     {2, [0]}
    ].
