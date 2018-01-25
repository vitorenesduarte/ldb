%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
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
         members/0,
         update_membership/1,
         send/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {members :: list(ldb_node_id()),
                mm_fun :: function(),
                metrics :: boolean()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec members() -> list(ldb_node_id()).
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec update_membership(list(node_spec())) -> ok.
update_membership(Membership) ->
    gen_server:cast(?MODULE, {update_membership, Membership}).

-spec send(ldb_node_id(), term()) -> ok.
send(LDBId, Message) ->
    gen_server:cast(?MODULE, {send, LDBId, Message}).

%% gen_server callbacks
init([]) ->
    case ldb_config:get(ldb_mode, ?DEFAULT_MODE) of
        state_based ->
            schedule_state_sync();
        delta_based ->
            schedule_state_sync();
        pure_op_based ->
            ok
    end,

    %% configure membership callback
    MembershipFun = fun(Membership) ->
        ldb_whisperer:update_membership(Membership)
    end,
    partisan_peer_service:add_sup_callback(MembershipFun),

    lager:info("ldb_whisperer initialized!"),
    {ok, #state{members=[],
                mm_fun=ldb_backend:message_maker(),
                metrics=ldb_config:get(ldb_metrics)}}.

handle_call(members, _From, #state{members=Members}=State) ->
    ldb_util:qs("WHISPERER members"),
    {reply, Members, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({update_membership, Membership}, State) ->
    ldb_util:qs("WHISPERER update_membership"),
    Members = [Name || {Name, _, _} <- Membership, Name /= ldb_config:id()],

    lager:info("NEW MEMBERS ~p\n", [Members]),

    {noreply, State#state{members=Members}};

handle_cast({send, LDBId, Message}, #state{metrics=Metrics}=State) ->
    ldb_util:qs("WHISPERER send"),
    do_send(LDBId, Message, Metrics),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(state_sync, #state{members=LDBIds,
                               mm_fun=MessageMakerFun,
                               metrics=Metrics}=State) ->
    ldb_util:qs("WHISPERER state_sync"),

    FoldFunction = fun({Key, Value}, _) ->
        lists:foreach(
            fun(LDBId) ->

                Message = do_make(MessageMakerFun, Key, Value, LDBId, Metrics),

                %% send message if there's a message to send
                case Message of
                    nothing ->
                        ok;
                    _ ->
                        do_send(LDBId, Message, Metrics)
                end

            end,
            LDBIds
        )
    end,

    ldb_store:fold(FoldFunction, undefined),
    schedule_state_sync(),
    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
schedule_state_sync() ->
    Interval = ldb_config:get(ldb_state_sync_interval),
    timer:send_after(Interval, state_sync).

%% @private
do_make(MessageMakerFun, Key, Value, LDBId, true) ->
    {MicroSeconds, Message} = timer:tc(
        MessageMakerFun,
        [Key, Value, LDBId]
    ),

    %% record latency creating this message
    ldb_metrics:record_latency(local, MicroSeconds),

    Message;

do_make(MessageMakerFun, Key, Value, LDBId, false) ->
    MessageMakerFun(Key, Value, LDBId).

%% @private
-spec do_send(ldb_node_id(), term(), boolean()) -> ok.
do_send(LDBId, Message, Metrics) ->
    %% try to send the message
    Result = ldb_peer_service:forward_message(
        LDBId,
        ldb_listener,
        Message
    ),

    %% if message was sent, collect metrics
    case Result of
        ok ->
            case Metrics of
                true ->
                    metrics(Message);
                false ->
                    ok
            end;
        Error ->
            lager:info("Error trying to send message ~p to node ~p. Reason ~p",
                       [Message, LDBId, Error])
    end,
    ok.

%% @private
%% state-based
metrics({_Key, state, CRDT}) ->
    M = {state, ldb_util:size(crdt, CRDT)},
    record_message([M]);
metrics({_Key, digest, _From, _Bottom, {state, CRDT}}) ->
    M = {state, ldb_util:size(crdt, CRDT)},
    record_message([M]);
metrics({_Key, digest, _From, _Bottom, {mdata, Digest}}) ->
    M = {digest, ldb_util:size(term, Digest)},
    record_message([M]);
metrics({_Key, digest_and_state, _From, Delta, {mdata, Digest}}) ->
    M1 = {state, ldb_util:size(crdt, Delta)},
    M2 = {digest, ldb_util:size(term, Digest)},
    record_message([M1, M2]);
%% delta-based
metrics({_Key, delta, _From, Sequence, Delta}) ->
    M1 = {delta_ack, ldb_util:size(term, Sequence)},
    M2 = {delta, ldb_util:size(crdt, Delta)},
    record_message([M1, M2]);
metrics({_Key, delta_ack, _From, Sequence}) ->
    M = {delta_ack, ldb_util:size(term, Sequence)},
    record_message([M]);
metrics({_Key, digest, _From, Sequence, _Bottom, {state, CRDT}}) ->
    M1 = {delta_ack, ldb_util:size(term, Sequence)},
    M2 = {state, ldb_util:size(crdt, CRDT)},
    record_message([M1, M2]);
metrics({_Key, digest, _From, Sequence, _Bottom, {mdata, Digest}}) ->
    M1 = {delta_ack, ldb_util:size(term, Sequence)},
    M2 = {digest, ldb_util:size(term, Digest)},
    record_message([M1, M2]);
metrics({_Key, digest_and_state, _From, Sequence, Delta, {mdata, Digest}}) ->
    M1 = {delta_ack, ldb_util:size(term, Sequence)},
    M2 = {state, ldb_util:size(crdt, Delta)},
    M3 = {digest, ldb_util:size(term, Digest)},
    record_message([M1, M2, M3]).

%% @private
record_message(L) ->
    lists:foreach(
        fun({Type, Size}) ->
            ldb_metrics:record_message(Type, Size)
        end,
        L
    ).
