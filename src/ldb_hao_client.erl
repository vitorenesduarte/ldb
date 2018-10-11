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


-module(ldb_hao_client).
-author("Vitor Enes <vitorenesduarte@gmail.com>").

-include("ldb.hrl").

-behaviour(gen_server).
-behaviour(ranch_protocol).

%% API
-export([start_link/3,
         start_link/4]).

%% gen_server
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(state, {socket :: inet:socket(),
                spec :: node_spec() | undefined}).


%% @doc Ranch callback when a new connection
%%      is accepted.
start_link(Ref, Socket, ranch_tcp, []) ->
    Arg = [in, Ref, Socket],
    {ok, proc_lib:spawn_link(?MODULE,
                             init,
                             [Arg])}.

%% @doc To be used when connecting to a new actor
%%      with IP address `Ip' on TCP port `Port'.
start_link(Name, Ip, Port) ->
    Arg = [out, Ip, Port],
    gen_server:start_link({local, Name},
                          ?MODULE,
                          Arg,
                          []).


%% @doc Implementation of `ranch_protocol' using `gen_server'.
%%      See:
%%       - https://n:inenines.eu/docs/en/ranch/1.3/guide/protocols/#_using_gen_server
%%       - https://github.com/ninenines/ranch/blob/master/examples/tcp_reverse/src/reverse_protocol.erl
init([in, Ref, Socket]) ->
    lager:info("New connection ~p", [Socket]),

    %% configure socket
    ok = ranch:accept_ack(Ref),
    ok = ldb_socket:configure(Socket),

    %% don't be rude and accept the hello
    Spec = receive_hello(Socket),

    gen_server:enter_loop(?MODULE,
                          [],
                          #state{socket=Socket,
                                 spec=Spec});

init([out, Ip, Port]) ->
    lager:info("Connecting to ~p on port ~p", [Ip, Port]),

    case ldb_socket:connect(Ip, Port) of
        {ok, Socket} ->
            %% configure socket
            ok = ldb_socket:configure(Socket),

            %% be nice and say hello
            say_hello(Socket),

            {ok, #state{socket=Socket,
                        spec=undefined}};

        _Error ->
            {stop, normal}
    end.

handle_call(Msg, _From, State) ->
    {stop, {unhandled, Msg}, State}.

handle_cast({forward_message, _, _}=Data, #state{socket=Socket}=State) ->
    ldb_util:qs(do_send),
    Encoded = term_to_binary(Data),
    do_send(Encoded, Socket),
    {noreply, State}.

handle_info({tcp, Socket, Encoded}, State) ->
    Data = binary_to_term(Encoded),
    do_receive(Data, Socket),
    {noreply, State};

handle_info({tcp_closed, Socket}, State) ->
    %% TODO log process id
    lager:info("TCP closed ~p", [Socket]),
    {stop, normal, State}.

terminate(Reason, #state{socket=Socket, spec={Id, _, _}}) ->
    lager:info("Terminate. Reason ~p", [Reason]),

    ok = gen_tcp:close(Socket),
    ldb_hao:exit(Id, self()),

    ok.

%% @private
do_receive({forward_message, Mod, Message}, Socket) ->
    %% forward to mod/actor
    gen_server:cast(Mod, Message),

    %% reactivate socket
    ok = ldb_socket:activate(Socket).

%% @private
do_send(Encoded, Socket) ->
    ldb_socket:send(Socket, Encoded).

%% @private
say_hello(Socket) ->
    Message = {hello, ldb_hao:myself()},
    Encoded = term_to_binary(Message),
    do_send(Encoded, Socket).

%% @private
receive_hello(Socket) ->
    %% receive message
    {ok, Data} = ldb_socket:recv(Socket),
    {hello, Spec} = binary_to_term(Data),
    lager:info("received hello ~p", [Spec]),

    %% reactivate socket
    ok = ldb_socket:activate(Socket),
    Spec.
