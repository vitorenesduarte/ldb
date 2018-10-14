%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Vitor Enes.  All Rights Reserved.
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

%% @doc hao connections module.

-module(ldb_hao_connections).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

%% API
-export([new/0,
         connect/4,
         members/1,
         exit/3]).

-export_type([connections/0]).


%% connections
-record(connection, {ip :: node_ip(),
                     port :: node_port(),
                     pids :: [pid()]}).
-type connection() :: #connection{}.
-type connections() :: maps:map(ldb_node_id(), connection()).

%% @doc Create a new connections structure.
-spec new() -> connections().
new() ->
    maps:new().

%% @doc Try to connect with another actor.
%%      It attempts to connect with this actor if:
%%        - never connected
%%        - was connected but is now disconnected
%%      It returns a tuple:
%%        - 1st component: ok or connection error
%%        - 2nd component: boolean indicating if the list of members changed
%%        - 3nd component: connections
-spec connect(ldb_node_id(), node_ip(), node_port(), connections()) ->
    {ok | error(), connections()}.
connect(Id, Ip, Port, Connections) ->
    do_connect(Id, Ip, Port, Connections, ?CONNECTIONS).

%% @doc Returns a list of connected members.
-spec members(connections()) -> list(ldb_node_id()).
members(Connections) ->
    maps:fold(
        fun(Id, #connection{pids=Pids}, Result) ->
            case Pids of
                [] -> Result;
                _ -> [Id | Result]
            end
        end,
        [],
        Connections
    ).

%% @doc Marks the entry associated with `Id' as `undefined'.
%%      It also informs if the set of members changed:
%%        - if was already `undefined', it didn't (can this every happen?)
%%        - if not, it did :)
-spec exit(ldb_node_id(), pid(), connections()) -> {boolean(), connections()}.
exit(Id, Pid, Connections0) ->
    #connection{pids=Pids0}=Connection = maps:get(Id, Connections0),
    Pids = Pids0 -- [Pid],
    {Pids == [], maps:put(Id, Connection#connection{pids=Pids}, Connections0)}.

%% @private
-spec do_connect(ldb_node_id(), node_ip(), node_port(), connections(), non_neg_integer()) ->
    {ok | error(), connections()}.
do_connect(_, _, _, Connections, 0) ->
    {ok, Connections};
do_connect(Id, Ip, Port, Connections0, N) ->
    Name = ldb_util:connection_name(Id, N),
    case ldb_hao_client:start_link(Name, Ip, Port) of
        {ok, Pid} ->
            Connections = maps:update_with(
                Id,
                fun(#connection{pids=Pids}=Connection) ->
                    Connection#connection{pids=[Pid | Pids]}
                end,
                #connection{ip=Ip, port=Port, pids=[Pid]},
                Connections0
            ),
            do_connect(Id, Ip, Port, Connections, N - 1);
        Error ->
            {Error, Connections0}
    end.
