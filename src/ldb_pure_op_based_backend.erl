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

-module(ldb_pure_op_based_backend).
-author("Georges Younes <georges.r.younes@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2,
         message_maker/0,
         message_handler/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         update_stablevv/1]).

-record(state, {actor :: non_neg_integer(),
                vc :: vclock(),
                svc :: vclock(),
                rtm :: mclock(),
                to_be_delivered_queue :: [{non_neg_integer(), message(), vclock()}],
                to_be_ack_queue :: [{{non_neg_integer(), vclock()}, message(), non_neg_integer(), integer(), [non_neg_integer()]}]}).

-define(WAIT_TIME_BEFORE_RESEND, 5).
-define(CHECK_RESEND_INTERVAL, 5000).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(key(), type()) -> ok.
create(Key, Type) ->
    gen_server:call(?MODULE, {create, Key, Type}, infinity).

-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

-spec update(key(), operation()) -> ok | not_found() | error().
update(Key, Operation) ->
    gen_server:call(?MODULE, {update, Key, Operation}, infinity).

%% Deliver a message.
-spec tcbdeliver(atom(), message(), vclock()) -> ok.
tcbdeliver(MessageActor, MessageBody, MessageVClock) ->
    gen_server:cast(?MODULE, {tcbdeliver, MessageActor, MessageBody, MessageVClock}).

-spec message_maker() -> function().
message_maker() ->
    fun(_, _, _) ->
        ldb_log:warning("message_maker called and it was not supposed to")
    end.

-spec message_handler(term()) -> function().
message_handler(_) ->
    fun(M) ->
        gen_server:cast(?MODULE, M)
    end.

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    %% Generate local version vector.
    VClock = orddict:new(),

    %% Generate local stable version vector.
    SVC = orddict:new(),

    %% Generate local recent timestamp matrix.
    RTM = orddict:new(),

    %% Generate local to be delivered messages queue.
    ToBeDeliveredQueue = [],

    %% Generate local to be delivered messages queue.
    ToBeAckQueue = [],

    %% Start check resend timer.
    schedule_check_resend(),

    ldb_log:info("ldb_pure_op_based_backend initialized!", extended),

    {ok, #state{actor=Actor,
                vc=VClock,
                svc=SVC,
                rtm=RTM,
                to_be_delivered_queue=ToBeDeliveredQueue,
                to_be_ack_queue=ToBeAckQueue}}.

handle_call({create, Key, LDBType}, _From, State) ->
    Type = ldb_util:get_type(LDBType),
    %% @todo support complex types
    Result = ldb_store:create(Key, Type:new()),
    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {Type, _}=CRDT} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,
    {reply, Result, State};

handle_call({update, Key, Operation} = MessageBody, _From, #state{actor=Actor, vc=VC0, to_be_ack_queue=ToBeAckQueue0}=State) ->

    %% Increment vclock.
    VC1 = increment_vclock(Actor, VC0),

    %% Get neighbours
    {ok, Members} = ldb_peer_service:members(),

    %% Generate list of peers that need the message (neighbours minus self)
    ToMembers = Members -- [Actor],

    %% Get current time.
    Now = ldb_util:timestamp(),

    %% Add members to the queue of not acked messages.
    ToBeAckQueue1 = ToBeAckQueue0 ++ [{{Actor, VC1}, MessageBody, Actor, Now, ToMembers}],

    %% Generate message.
    Msg1 = {tcbcast, {Key, encode_op(Operation)}, Actor, VC1, Actor},

    %% Send Message.
    [ldb_whisperer:send(Peer, Msg1) || Peer <- ToMembers],

    Function = fun({Type, _}=CRDT) ->
        Type:mutate(Operation, VC1, CRDT)
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State#state{vc=VC1, to_be_ack_queue=ToBeAckQueue1}};

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({tcbcast, {Key, OperationCode}, MessageActor, MessageVC, Sender},
    #state{actor=Actor, vc=VC0, to_be_ack_queue=ToBeAckQueue0, to_be_delivered_queue=ToBeDeliveredQueue0} = State) ->
        MessageBody = {Key, decode_op(OperationCode)},
        case already_seen_message(MessageVC, VC0, ToBeDeliveredQueue0) of
            true ->
                %% Already seen, do nothing.
                ldb_log:info("Ignoring duplicate message from cycle."),
                {noreply, State};
            false ->
                %% Get neighbours
                {ok, Members} = ldb_peer_service:members(),

                %% Generate list of peers that need the message (neighbours minus message sender, self and message creator).
                ToMembers = Members -- [Sender, Actor, MessageActor],

                ldb_log:info("Broadcasting message to peers: ~p", [ToMembers]),

                %% Generate message.
                Msg = {tcbcast, {Key, OperationCode}, MessageActor, MessageVC, Actor},

                %% Send Message.
                [ldb_whisperer:send(Peer, Msg) || Peer <- ToMembers],

                %% Get current time.
                Now = ldb_util:timestamp(),

                %% Generate message.
                MessageAck = {tcbcast_ack, MessageActor, MessageVC, Actor},

                %% Send ack back to message sender.
                ldb_whisperer:send(Sender, MessageAck),

                %% Attempt to deliver locally if we received it on the wire.
                tcbdeliver(MessageActor, MessageBody, MessageVC),

                %% Add members to the queue of not ack messages and increment the vector clock.
                ToBeAckQueue1 = ToBeAckQueue0 ++ [{{MessageActor, MessageVC}, MessageBody, Sender, Now, ToMembers}],

                {noreply, State#state{to_be_ack_queue=ToBeAckQueue1}}

        end;

handle_cast({tcbcast_ack, MessageActor, MessageVC, Sender}, #state{to_be_ack_queue=ToBeAckQueue0} = State) ->
    %% Get list of waiting ackwnoledgements.
    {_, MessageBody, From, Timestamp, Members0} = lists:keyfind({MessageActor, MessageVC},
                                             1,
                                             ToBeAckQueue0),

    %% Remove this member as an outstanding member.
    Members = lists:delete(Sender, Members0),

    ToBeAckQueue1 = case length(Members) of
        0 ->
            %% None left, remove from ack queue.
            lists:keydelete({MessageActor, MessageVC}, 1, ToBeAckQueue0);
        _ ->
            %% Still some left, preserve.
            lists:keyreplace({MessageActor, MessageVC},
                             1,
                             ToBeAckQueue0, {{MessageActor, MessageVC}, MessageBody, From, Timestamp, Members})
    end,

    {noreply, State#state{to_be_ack_queue=ToBeAckQueue1}};

handle_cast({tcbdeliver, MessageActor, MessageBody, MessageVC},
    #state{vc=VC0, rtm=RTM0, to_be_delivered_queue=ToBeDeliveredQueue0} = State) ->

    %% Check if the message should be delivered and delivers it or not.
    {VC1, ToBeDeliveredQueue1} = causal_delivery({MessageActor, MessageBody, MessageVC},
                                           VC0,
                                           ToBeDeliveredQueue0),

    %% Update the Recent Timestamp Matrix.
    RTM1 = update_rtm(RTM0, MessageActor, MessageVC),

    %% Update the Stable Version Vector.
    %SVV1 = update_stablevv(RTM1),

    {noreply, State#state{vc=VC1, rtm=RTM1, to_be_delivered_queue=ToBeDeliveredQueue1}};

handle_cast(check_resend, #state{to_be_ack_queue=ToBeAckQueue0} = State) ->
    Now = ldb_util:timestamp(),
    ToBeAckQueue1 = lists:foldl(
        fun({{MessageActor, VClock}, {Key, Op} = MessageBody, Sender, Timestamp, MembersList}, ToBeAckQueue) ->
            case MembersList =/= [] andalso (Now - Timestamp > ?WAIT_TIME_BEFORE_RESEND) of
                true ->
                    Message1 = {tcbcast, {Key, encode_op(Op)}, MessageActor, VClock, Sender},
                    [ldb_whisperer:send(Peer, Message1) || Peer <- MembersList],
                    lists:keyreplace({MessageActor, VClock}, 1, ToBeAckQueue, {{MessageActor, VClock}, MessageBody, Sender, ldb_util:timestamp(), MembersList});
                false ->
                    %% Do nothing.
                    ToBeAckQueue
            end
        end,
    ToBeAckQueue0,
    ToBeAckQueue0),

    schedule_check_resend(),

    {noreply, State#state{to_be_ack_queue=ToBeAckQueue1}};

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

%% @private
increment_vclock(Node, VClock) ->
    Ctr = case orddict:find(Node, VClock) of
        {ok, Ctr0} ->
            Ctr0 + 1;
        error ->
            1
    end,
    orddict:store(Node, Ctr, VClock).

%% @private
update_rtm(RTM, MsgActor, MsgVV) ->
    orddict:store(MsgActor, MsgVV, RTM).

%% @private
update_stablevv(RTM0) ->
    [{_, Min0} | RTM1] = RTM0,

    lists:foldl(
        fun({_, VV}, Acc) ->
            lists:foldl(
                fun({Actor, Count}, Acc2) ->
                    case Count < get_counter(Actor, Acc2) of
                        true ->
                            lists:keyreplace(Actor, 1, Acc2, {Actor, Count});
                        false ->
                            Acc2
                    end
                end,
                Acc,
                VV
            )
        end,
        Min0,
        RTM1
    ).

%% @private
get_counter(Node, VClock) ->
    case lists:keyfind(Node, 1, VClock) of
        {_, Ctr} ->
            Ctr;
        false ->
            0
    end.

%% @private
equals_vclock(VClock1, VClock2) ->
    Fun = fun(Value1, Value2) -> Value1 == Value2 end,
    orddict_ext:equal(VClock1, VClock2, Fun).

%% @private
happened_before(VClock1, VClock2) ->
    pure_trcb:happened_before(VClock1, VClock2).

%% @private
can_be_delivered(MsgVClock, NodeVClock, Origin) ->
    orddict:fold(
        fun(Key, Value, Acc) ->
            case orddict:find(Key, NodeVClock) of
                {ok, NodeVCValue} ->
                    case Key =:= Origin of
                        true ->
                            Acc and (Value =:= NodeVCValue + 1);
                        false ->
                            Acc and (Value =< NodeVCValue)
                    end;
                error ->
                    true and Acc
            end
        end,
        true,
        MsgVClock
    ).

%% @private
already_seen_message(MessageVC, NodeVC, ToBeDeliveredQueue) ->
    case happened_before(MessageVC, NodeVC) orelse
        equals_vclock(MessageVC, NodeVC) orelse
            in_to_be_delivered_queue(MessageVC, ToBeDeliveredQueue) of
                        true ->
                            true;
                        false ->
                            false
    end.

%% @private
in_to_be_delivered_queue(MsgVC, ToBeDeliveredQueue) ->
    lists:keymember(MsgVC, 3, ToBeDeliveredQueue).

%% @doc check if a message should be deliver and deliver it, if not add it to the queue
causal_delivery({Origin, {Key, Operation} = MessageBody, MessageVClock}, VV, Queue) ->
    case can_be_delivered(MessageVClock, VV, Origin) of
        true ->
            Function = fun({Type, _}=CRDT) ->
                Type:mutate(Operation, MessageVClock, CRDT)
            end,
            case ldb_store:update(Key, Function) of
                ok ->
                    NewVV = increment_vclock(Origin, VV),
                    try_to_deliever(Queue, {NewVV, Queue});
                _ ->
                    {VV, Queue ++ [{Origin, MessageBody, MessageVClock}]}
            end;
        false ->
            {VV, Queue ++ [{Origin, MessageBody, MessageVClock}]}
    end.

%% @doc Check for all messages in the queue to be delivered
%% Called upon delievery of a new message that could affect the delivery of messages in the queue
try_to_deliever([], {VV, Queue}) -> {VV, Queue};
try_to_deliever([{Origin, {Key, Operation}, MessageVClock}=El | RQueue], {VV, Queue}=V) ->
    case can_be_delivered(MessageVClock, VV, Origin) of
        true ->
            Function = fun({Type, _}=CRDT) ->
                Type:mutate(Operation, MessageVClock, CRDT)
            end,
            case ldb_store:update(Key, Function) of
                ok ->
                    NewVV = increment_vclock(Origin, VV),
                    Queue1 = lists:delete(El, Queue),
                    try_to_deliever(Queue1, {NewVV, Queue1});
                _ ->
                    try_to_deliever(RQueue, V)
            end;
        false ->
            try_to_deliever(RQueue, V)
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
encode_op({add, E}) ->
    {1, E}.

%% @private
decode_op({1, E}) ->
    {add, E}.

%% @private
schedule_check_resend() ->
    timer:send_after(?CHECK_RESEND_INTERVAL, check_resend).
