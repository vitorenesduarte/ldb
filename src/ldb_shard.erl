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

-module(ldb_shard).
-author("Vitor Enes <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_whisperer callbacks
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

-record(state, {shard_name :: atom(),
                actor :: ldb_node_id(),
                kv :: maps:map(key(), backend_stored()),
                backend :: atom(),
                backend_state :: backend_state(),
                state_sync_interval :: non_neg_integer(),
                members :: list(ldb_node_id()),
                ignore_keys :: sets:set(string())}).

-define(STATE_SYNC, state_sync).
-define(TIME_SERIES, time_series).
-define(TIME_SERIES_INTERVAL, 1000). %% 1 second.


-spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(ShardName) ->
    gen_server:start_link({local, ShardName}, ?MODULE, [ShardName], []).

%% gen_server callbacks
init([ShardName]) ->
    Actor = ldb_config:id(),
    KV = maps:new(),
    Backend = ldb_util:get_backend(),
    BackendState = Backend:backend_state(),
    Interval = ldb_config:get(ldb_state_sync_interval),
    Members = [],
    IgnoreKeys = sets:new(),

    %% schedule periodic events
    schedule_state_sync(Interval),
    schedule_time_series(),

    lager:info("ldb_shard ~p initialized!", [ShardName]),
    {ok, #state{shard_name=ShardName,
                actor=Actor,
                kv=KV,
                backend=Backend,
                backend_state=BackendState,
                state_sync_interval=Interval,
                members=Members,
                ignore_keys=IgnoreKeys}}.

handle_call({create, Key, LDBType}, _From, #state{kv=KV0,
                                                  backend=Backend,
                                                  backend_state=BackendState}=State) ->
    Bottom = ldb_util:new_crdt(type, LDBType),
    Default = Backend:bottom_entry(Bottom, BackendState),
    KV = maps:put(Key, Default, KV0),
    {reply, ok, State#state{kv=KV}};

handle_call({query, Key, Args}, _From, #state{kv=KV,
                                        backend=Backend}=State) ->
    Stored = maps:get(Key, KV),
    {Type, _}=CRDT = Backend:crdt(Stored),
    Result = case Args of
        [] -> Type:query(CRDT);
        _ -> Type:query(Args, CRDT)
    end,
    {reply, {ok, Result}, State};

handle_call({update, Key, Operation}, _From, #state{kv=KV0,
                                                    backend=Backend,
                                                    backend_state=BackendState}=State) ->
    KV = maps:update_with(
        Key,
        fun(Stored) -> Backend:update(Stored, Operation, BackendState) end,
        KV0
    ),
    {reply, ok, State#state{kv=KV}};

handle_call({update_ignore_keys, IgnoreKeys}, _From, State) ->
    {reply, ok, State#state{ignore_keys=IgnoreKeys}};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({msg, From, Key, Message}, #state{shard_name=ShardName,
                                              actor=Actor,
                                              kv=KV0,
                                              backend=Backend,
                                              backend_state=BackendState,
                                              ignore_keys=IgnoreKeys}=State) ->
    %% metrics
    Metrics = should_save_key(Key, IgnoreKeys),

    %% get current value
    Stored0 = maps:get(Key, KV0),

    %% update it with remote message and measure the time
    {MicroSeconds, {Stored, Reply}} = timer:tc(
        Backend,
        message_handler,
        [Message, From, Stored0, BackendState]
    ),

    %% send reply
    case Reply of
        nothing -> ok;
        _ -> do_send(Backend, ShardName, Actor, From, Key, Reply, Metrics)
    end,

    %% update with new value
    KV = maps:put(Key, Stored, KV0),

    %% maybe save metrics
    case Metrics of
        true -> ldb_metrics:record_processing(MicroSeconds);
        false -> ok
    end,
    {noreply, State#state{kv=KV}};

handle_cast({update_members, Members}, State) ->
    {noreply, State#state{members=Members}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(?STATE_SYNC, #state{shard_name=ShardName,
                                actor=Actor,
                                kv=KV,
                                backend=Backend,
                                backend_state=BackendState,
                                state_sync_interval=Interval,
                                members=LDBIds,
                                ignore_keys=IgnoreKeys}=State) ->
    FoldFun = fun(Key, Stored, _) ->
        %% if shouldn't ignore the key
        Metrics = should_save_key(Key, IgnoreKeys),
        lists:foreach(
            fun(LDBId) ->
                Message = do_make(Backend, BackendState, Stored, LDBId),

                %% send message if there's a message to send
                case Message of
                    nothing -> ok;
                    _ -> do_send(Backend, ShardName, Actor, LDBId, Key, Message, Metrics)
                end

            end,
            LDBIds
        )
    end,

    maps:fold(FoldFun, undefined, KV),
    schedule_state_sync(Interval),
    {noreply, State};

handle_info(?TIME_SERIES, #state{kv=KV,
                                 backend=Backend,
                                 ignore_keys=IgnoreKeys}=State) ->
    FoldFun = fun(Key, Stored, Acc) ->
        case should_save_key(Key, IgnoreKeys) of
            true -> ldb_util:two_plus(Acc, Backend:memory(Stored));
            false -> Acc
        end
    end,
    Result = maps:fold(FoldFun, {{0, 0}, {0, 0}}, KV),

    %% notify metrics, if there's something to notify
    ldb_metrics:record_memory(Result),
    schedule_time_series(),
    {noreply, State};

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec do_make(atom(), backend_state(), backend_stored(), ldb_node_id()) -> term().
do_make(Backend, BackendState, Stored, LDBId) ->
    {MicroSeconds, Message} = timer:tc(
        Backend,
        message_maker,
        [Stored, LDBId, BackendState]
    ),

    %% record time creating this message
    ldb_metrics:record_processing(MicroSeconds),

    Message.

%% @private
-spec do_send(atom(), atom(), ldb_node_id(), ldb_node_id(), key(), term(), boolean()) -> ok.
do_send(Backend, ShardName, From, To, Key, Message, Metrics) ->
    %% try to send the message
    Result = ldb_peer_service:forward_message(
        To,
        ShardName,
        {msg, From, Key, Message}
    ),

    %% if message was sent, collect metrics
    case Result of
        ok ->
            case Metrics of
                true ->
                    Size = Backend:message_size(Message),
                    ldb_metrics:record_transmission(Size);
                false ->
                    ok
            end;
        Error ->
            lager:info("Error trying to send message ~p to node ~p. Reason ~p",
                       [Message, To, Error])
    end,
    ok.

%% @private
-spec should_save_key(string(), sets:set(string())) -> boolean().
should_save_key(Key, IgnoreKeys) ->
    not sets:is_element(Key, IgnoreKeys).

%% @private
schedule_time_series() ->
    timer:send_after(?TIME_SERIES_INTERVAL, ?TIME_SERIES).

%% @private
schedule_state_sync(Interval) ->
    timer:send_after(Interval, ?STATE_SYNC).