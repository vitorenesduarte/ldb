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

-module(ldb_whisperer).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_whisperer callbacks
-export([start_link/0,
         send/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(SYNC_INTERVAL, 5000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec send(node_name(), term()) -> ok.
send(NodeName, Message) ->
    gen_server:cast(?MODULE, {send, NodeName, Message}).

%% gen_server callbacks
init([]) ->
    case ldb_config:mode() of
        state_based ->
            schedule_sync();
        delta_based ->
            schedule_sync()
    end,

    ldb_log:info("ldb_whisperer initialized!", extended),
    {ok, #state{}}.

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({send, NodeName, Message}, State) ->
    do_send(NodeName, Message),
    {noreply, State};

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(sync, State) ->
    {ok, NodeNames} = ldb_peer_service:members(),

    FoldFunction = fun({Key, Value}, _Acc) ->
        lists:foreach(
            fun(NodeName) ->
                MessageMakerFun = ldb_backend:message_maker(),
                case MessageMakerFun(Key, Value, NodeName) of
                    {ok, Message} ->
                        do_send(NodeName, Message);
                    nothing ->
                        ok
                end
            end,
            NodeNames
        )
    end,

    ldb_store:fold(FoldFunction, undefined),
    schedule_sync(),
    {noreply, State};

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_sync() ->
    timer:send_after(?SYNC_INTERVAL, sync).

%% @private
do_send(NodeName, Message) ->
    log_transmission(Message),
    Result = ldb_peer_service:forward_message(
        NodeName,
        {ldb_listener, handle_message},
        Message
    ),

    case Result of
        ok ->
            ok;
        Error ->
            ldb_log:info("Error trying to send message ~p to node ~p. Reason ~p", [Message, NodeName, Error])
    end.

%% @private
log_transmission({_Key, state_send, CRDT}) ->
    log_transmission(state_send, CRDT);
log_transmission({_Key, delta_send, From, Sequence, Delta}) ->
    log_transmission(delta_send, {From, Sequence, Delta});
log_transmission({_Key, delta_ack, From, Sequence}) ->
    log_transmission(delta_ack, {From, Sequence}).
log_transmission(Type, Payload) ->
    ldb_instrumentation:transmission(Type, Payload).
