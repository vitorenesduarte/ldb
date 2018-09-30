%%
%% Copyright (c) 2016-2018 Vitor Enes.  All Rights Reserved.
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
         update_members/1,
         update_metrics_members/1,
         update_ignore_keys/1,
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
                metrics :: boolean(),
                %% if the following is different that `all',
                %% transmission metrics will only be recorded
                %% for peers with ids in the set
                metrics_members :: all | sets:set(ldb_node_id()),
                ignore_keys :: sets:set(string())}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec members() -> list(ldb_node_id()).
members() ->
    gen_server:call(?MODULE, members, infinity).

-spec update_members(list(ldb_node_id())) -> ok.
update_members(Members) ->
    gen_server:cast(?MODULE, {update_members, Members}).

-spec update_metrics_members(list(ldb_node_id())) -> ok.
update_metrics_members(Members) ->
    gen_server:cast(?MODULE, {update_metrics_members, Members}).

-spec update_ignore_keys(sets:set(string())) -> ok.
update_ignore_keys(IgnoreKeys) ->
    gen_server:call(?MODULE, {update_ignore_keys, IgnoreKeys}, infinity).

-spec send(ldb_node_id(), term()) -> ok.
send(LDBId, Message) ->
    gen_server:cast(?MODULE, {send, LDBId, Message}).

%% gen_server callbacks
init([]) ->
    %% always schedule state sync
    schedule_state_sync(),

    %% configure members callback
    MembersFun = fun(Membership) ->
        Members = ldb_util:parse_membership(Membership),
        ldb_whisperer:update_members(Members)
    end,
    partisan_peer_service:add_sup_callback(MembersFun),

    lager:info("ldb_whisperer initialized!"),
    {ok, #state{members=[],
                mm_fun=ldb_backend:message_maker(ldb_backend:backend_state()),
                metrics=ldb_config:get(ldb_metrics),
                metrics_members=all,
                ignore_keys=sets:new()}}.

handle_call(members, _From, #state{members=Members}=State) ->
    ldb_util:qs("WHISPERER members"),
    {reply, Members, State};

handle_call({update_ignore_keys, IgnoreKeys}, _Fromm, State) ->
    ldb_util:qs("WHISPERER update_ignore_keys"),
    {reply, ok, State#state{ignore_keys=IgnoreKeys}};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({update_members, Members}, State) ->
    ldb_util:qs("WHISPERER update_members"),
    lager:info("NEW MEMBERS ~p\n", [Members]),

    {noreply, State#state{members=Members}};

handle_cast({update_metrics_members, Members}, State) ->
    ldb_util:qs("WHISPERER update_metrics_members"),
    lager:info("NEW METRICS MEMBERS ~p\n", [Members]),

    {noreply, State#state{metrics_members=sets:from_list(Members)}};

handle_cast({send, LDBId, Message}, #state{metrics=Metrics,
                                           metrics_members=MetricsMembers}=State) ->
    ldb_util:qs("WHISPERER send"),
    do_send(LDBId, Message, Metrics, MetricsMembers),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(state_sync, #state{members=LDBIds,
                               mm_fun=MessageMakerFun,
                               metrics=Metrics0,
                               metrics_members=MetricsMembers,
                               ignore_keys=IgnoreKeys}=State) ->
    ldb_util:qs("WHISPERER state_sync"),

    FoldFunction = fun(Key, Value, _) ->
        %% if metrics and shouldn't ignore the key
        Metrics = Metrics0 andalso should_save_key(Key, IgnoreKeys),
        lists:foreach(
            fun(LDBId) ->
                Message = do_make(MessageMakerFun, Key, Value, LDBId, Metrics),

                %% send message if there's a message to send
                case Message of
                    nothing -> ok;
                    _ -> do_send(LDBId, Message, Metrics, MetricsMembers)
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
-spec do_send(ldb_node_id(), term(), boolean(), sets:set(ldb_node_id())) -> ok.
do_send(LDBId, Message, Metrics, MetricsMembers) ->
    %% try to send the message
    Result = ldb_peer_service:forward_message(
        LDBId,
        ldb_listener,
        Message
    ),

    %% if message was sent, collect metrics
    case Result of
        ok ->
            RecordMetrics = Metrics andalso should_save_member(LDBId, MetricsMembers),
            case RecordMetrics of
                true -> record(metrics(Message));
                false -> ok
            end;
        Error ->
            lager:info("Error trying to send message ~p to node ~p. Reason ~p",
                       [Message, LDBId, Error])
    end,
    ok.

%% @private
-spec should_save_member(ldb_node_id(), all | sets:set(ldb_node_id())) -> boolean().
should_save_member(_LDBId, all) ->
    true;
should_save_member(LDBId, Set) ->
    sets:is_element(LDBId, Set).

%% @private
-spec should_save_key(string(), sets:set(string())) -> boolean().
should_save_key(Key, IgnoreKeys) ->
    not sets:is_element(Key, IgnoreKeys).

-define(SEQ, {0, 0}).
%% @private
%% state-based
-spec metrics(term()) -> {non_neg_integer(), non_neg_integer()}.
metrics({_Key, state, CRDT}) ->
    ldb_util:size(crdt, CRDT);
%% delta-based
metrics({_Key, delta, _From, _Sequence, Deltas}) ->
    lists:foldl(
        fun(Delta, Acc) ->
            ldb_util:plus(
                Acc,
                ldb_util:size(crdt, Delta)
            )
        end,
        ?SEQ,
        Deltas
    );
metrics({_Key, delta_ack, _From, _Sequence}) ->
    ?SEQ;
%% scuttlebutt
metrics({_Key, matrix, _From, _Bottom, Matrix}) ->
    ldb_util:size(matrix, Matrix);
metrics({_Key, dotted_buffer, Buffer}) ->
    ldb_util:size(dotted_buffer, Buffer).

%% @private
record(Size) ->
    ldb_metrics:record_transmission(Size).
