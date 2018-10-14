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

%% @doc socket module.

-module(ldb_socket).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

%% API
-export([connect/2,
         configure/1,
         activate/1,
         send/2,
         recv/1]).

%% socket
-type socket() :: inet:socket().

%% tcp options
-define(TCP_ACTIVE_OPTION, {active, once}).
-define(TCP_OPTIONS,
        [{packet, 4}]).
         %% {nodelay, true},
         %% {keepalive, true}]).

%% @doc Connect to a server on TCP port `Port'
%%      on the host with IP address `Ip'.
-spec connect(node_ip(), node_port()) -> {ok, socket()} | error().
connect(Ip, Port) ->
    ranch_tcp:connect(Ip, Port, []).

%% @doc Set `?TCP_OPTIONS' on `Socket'.
-spec configure(socket()) -> ok.
configure(Socket) ->
    ranch_tcp:setopts(Socket, ?TCP_OPTIONS).

%% @doc Set `?TCP_ACTIVE_OPTION' on `Socket'.
-spec activate(socket()) -> ok.
activate(Socket) ->
    ranch_tcp:setopts(Socket, [?TCP_ACTIVE_OPTION]).

%% @doc Send `Message' on a `Socket'.
-spec send(socket(), iodata()) -> ok | error().
send(Socket, Message) ->
    ranch_tcp:send(Socket, Message).

%% @doc Receive a message from a `Socket'.
-spec recv(socket()) -> {ok, iodata()}.
recv(Socket) ->
    %% since we're packaging messages
    %% length is not relevant.
    %% @see http://erlang.org/doc/man/gen_tcp.html#recv-2
    Length = 0,
    Timeout = infinity,
    ranch_tcp:recv(Socket, Length, Timeout).
