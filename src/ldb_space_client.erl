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

-module(ldb_space_client).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

-define(OK, 0).
-define(KEY_ALREADY_EXISTS, 1).

%% ldb_space_client callbacks
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket :: gen_tcp:socket()}).

-spec start_link(gen_tcp:socket()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Socket) ->
    gen_server:start_link(?MODULE, [Socket], []).

init([Socket]) ->
    ldb_log:info("ldb_space_client initialized! Node ~p listening to new client ~p", [node(), Socket], extended),
    {ok, #state{socket=Socket}}.

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info({tcp, _, Data}, #state{socket=Socket}=State) ->
    handle_message(decode(Data), Socket),
    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
encode(Message) ->
    JSONBinary = jiffy:encode(Message),
    iolist_to_binary([JSONBinary, <<"\n">>]).

%% @private
decode(Message) ->
    jiffy:decode(Message).

%% @private
handle_message({Message}, Socket) ->
    {value, {_, Key0}} = lists:keysearch(<<"key">>, 1, Message),
    {value, {_, Method0}} = lists:keysearch(<<"method">>, 1, Message),

    Key = binary_to_list(Key0),
    Method = binary_to_atom(Method0),

    Reply = case Method of
        create ->
            {value, {_, Type0}} = lists:keysearch(<<"type">>, 1, Message),
            Type = binary_to_atom(Type0),
            case erlang:apply(ldb, create, [Key, Type]) of
                ok ->
                    {[{code, ?OK}]};
                {error, already_exists} ->
                    {[{code, ?KEY_ALREADY_EXISTS}]}
            end
    end,

    send(Reply, Socket).

%% @private
send(Reply, Socket) ->
    case gen_tcp:send(Socket, encode(Reply)) of
        ok ->
            ok;
        Error ->
            ldb_log:info("Failed to send message: ~p", [Error])
    end.

%% @private
binary_to_atom(B) ->
    binary_to_atom(B, utf8).
