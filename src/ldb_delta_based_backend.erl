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

-module(ldb_delta_based_backend).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2,
         update_members/1,
         message_maker/1,
         message_handler/2,
         memory/1,
         backend_state/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: ldb_node_id(),
                bp :: boolean(),
                rr :: boolean()}).
-type st() :: #state{}.

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

-spec update_members(list(ldb_node_id())) -> ok.
update_members(Members) ->
    gen_server:cast(?MODULE, {update_members, Members}).

-spec message_maker(st()) -> function().
message_maker(#state{actor=Actor}) ->
    fun(Key, {CRDT, DeltaBuffer, AckMap}, NodeName) ->

        %% get seq and last ack
        Seq = ldb_dbuffer:seq(DeltaBuffer),
        LastAck = last_ack(NodeName, AckMap),
        ?DEBUG("message_maker: DeltaBuffer ~p", [ldb_dbuffer:show(DeltaBuffer)]),
        ?DEBUG("message_maker: Seq ~p LastAck ~p NodeName ~p", [Seq, LastAck, NodeName]),

        case LastAck < Seq of
            true ->

                %% there's something missing in `NodeName'
                %% get min seq
                MinSeq = ldb_dbuffer:min_seq(DeltaBuffer),
                case ldb_dbuffer:is_empty(DeltaBuffer) orelse MinSeq > LastAck of
                    true ->
                        {
                            Key,
                            delta,
                            Actor,
                            Seq,
                            CRDT
                        };
                    false ->

                        %% should send deltas
                        Delta = ldb_dbuffer:select(NodeName, LastAck, DeltaBuffer),

                        ?DEBUG("message_maker: Deltas0 ~p", [Deltas0]),

                        case Delta of
                            undefined ->
                                nothing;
                            _ ->
                                {
                                    Key,
                                    delta,
                                    Actor,
                                    Seq,
                                    Delta
                                }
                        end
                end;

            false ->
                nothing
        end
    end.

-spec message_handler(term(), st()) -> function().
message_handler({_, delta, _, _, _}, #state{actor=Actor, bp=BP, rr=RR}) ->
    fun({Key, delta, From, N, {Type, _}=Remote}) ->
        ?DEBUG("delta: Remote ~p From ~p N ~p", [Remote, From, N]),

        %% create bottom entry
        Bottom = ldb_util:new_crdt(state, Remote),
        Default = get_entry(Bottom, BP),

        ldb_store:update(
            Key,
            fun({LocalCRDT0, DeltaBuffer0, AckMap}) ->
                ?DEBUG("delta: DeltaBuffer0 ~p", [ldb_dbuffer:show(DeltaBuffer0)]),

                {Delta, LocalCRDT} = Type:delta_and_merge(Remote, LocalCRDT0),

                DeltaBuffer = case Type:is_bottom(Delta) of
                    true ->
                        %% no inflation
                        DeltaBuffer0;
                    false ->
                        %% if inflation, add \Delta if RR, remote otherwise
                        case RR of
                            true -> ldb_dbuffer:add_inflation(Delta, From, DeltaBuffer0);
                            false -> ldb_dbuffer:add_inflation(Remote, From, DeltaBuffer0)
                        end
                end,

                ?DEBUG("delta: DeltaBuffer ~p", [ldb_dbuffer:show(DeltaBuffer)]),

                %% send ack
                Ack = {
                    Key,
                    delta_ack,
                    Actor,
                    N
                },
                ldb_whisperer:send(From, Ack),

                StoreValue = {LocalCRDT, DeltaBuffer, AckMap},
                {ok, StoreValue}
            end,
            Default
        )
    end;
message_handler({_, delta_ack, _, _}, _State) ->
    fun({Key, delta_ack, From, N}) ->
        ldb_store:update(
            Key,
            fun({LocalCRDT, DeltaBuffer0, AckMap0}) ->
                LastAck = last_ack(From, AckMap0),

                %% when a new ack is received,
                %% update the number of rounds without
                %% receiving an ack to 0
                MaxAck = max(LastAck, N),
                AckMap1 = maps:put(From, MaxAck, AckMap0),

                %% and try to shrink the delta-buffer immediately
                DeltaBuffer1 = ldb_dbuffer:prune(min_seq_ack_map(AckMap1), DeltaBuffer0),

                StoreValue = {LocalCRDT, DeltaBuffer1, AckMap1},
                {ok, StoreValue}
            end
        )
    end.

-spec memory(sets:set(string())) -> {size_metric(), size_metric()}.
memory(IgnoreKeys) ->
    gen_server:call(?MODULE, {memory, IgnoreKeys}, infinity).

-spec backend_state() -> st().
backend_state() ->
    gen_server:call(?MODULE, backend_state, infinity).


%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),

    %% config
    Actor = ldb_config:id(),
    BP = ldb_config:get(ldb_dgroup_back_propagation, false),
    RR = ldb_config:get(ldb_redundant_dgroups, false),

    %% configure members callback
    MembersFun = fun(Membership) ->
        Members = ldb_util:parse_membership(Membership),
        ldb_delta_based_backend:update_members(Members)
    end,
    partisan_peer_service:add_sup_callback(MembersFun),

    lager:info("ldb_delta_based_backend initialized!"),
    {ok, #state{actor=Actor,
                bp=BP,
                rr=RR}}.

handle_call({create, Key, LDBType}, _From, #state{bp=BP}=State) ->
    ldb_util:qs("DELTA BACKEND create"),
    Bottom = ldb_util:new_crdt(type, LDBType),
    Default = get_entry(Bottom, BP),

    Result = ldb_store:update(
        Key,
        fun(V) -> {ok, V} end,
        Default
    ),

    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    ldb_util:qs("DELTA BACKEND query"),
    Result = case ldb_store:get(Key) of
        {ok, {{Type, _}=CRDT, _, _}} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    ldb_util:qs("DELTA BACKEND update"),
    Function = fun({{Type, _}=CRDT0, DeltaBuffer0, AckMap}) ->
        case Type:delta_mutate(Operation, Actor, CRDT0) of
            {ok, Delta} ->
                CRDT1 = Type:merge(Delta, CRDT0),
                DeltaBuffer1 = ldb_dbuffer:add_inflation(Delta, Actor, DeltaBuffer0),
                StoreValue = {CRDT1, DeltaBuffer1, AckMap},
                {ok, StoreValue};
            Error ->
                Error
        end
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call({memory, IgnoreKeys}, _From, State) ->
    ldb_util:qs("DELTA BACKEND memory"),
    FoldFunction = fun(Key, {CRDT, DeltaBuffer, AckMap}, {C0, R0}) ->
       case sets:is_element(Key, IgnoreKeys) of
           true ->
               {C0, R0};
           false ->
                C = ldb_util:plus(C0, ldb_util:size(crdt, CRDT)),

                %% delta buffer + ack map
                R = ldb_util:plus([
                    R0,
                    ldb_util:size(ack_map, AckMap),
                    ldb_dbuffer:size(DeltaBuffer)
                ]),

                {C, R}
       end
    end,

    Result = ldb_store:fold(FoldFunction, {{0, 0}, {0, 0}}),
    {reply, Result, State};

handle_call(backend_state, _From, State) ->
    {reply, State, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({update_members, Peers}, State) ->
    ldb_util:qs("DELTA BACKEND update_members"),

    ShrinkFun = fun({_Key, {LocalCRDT, DeltaBuffer0, AckMap0}}) ->
        %% get buffer seq
        Sequence = ldb_dbuffer:seq(DeltaBuffer0),

        %% only keep in the ack map entries from current peers
        AckMap1 = maps:with(Peers, AckMap0),

        %% ensure all current peers have an entry in the ack map
        AllPeersInAckMap = lists:all(
            fun(Peer) ->
                maps:is_key(Peer, AckMap1)
            end,
            Peers
        ),

        %% if all peers are in the ack map,
        %% remove from the delta buffer all the entries
        %% acknowledged by all the peers
        DeltaBuffer1 = case AllPeersInAckMap of
            true ->
                Min = case length(Peers) of
                    0 ->
                        %% if no peers, delete all entries
                        %% in the delta buffer
                        Sequence;
                    _ ->
                        min_seq_ack_map(AckMap1)
                end,

                ldb_dbuffer:prune(Min, DeltaBuffer0);
            false ->
                DeltaBuffer0
        end,

        NewValue = {LocalCRDT, DeltaBuffer1, AckMap1},
        {ok, NewValue}
    end,

    ldb_store:update_all(ShrinkFun),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
get_entry(Bottom, BP) ->
    %% create buffer
    DeltaBuffer = ldb_dbuffer:new(BP),

    %% create ack map
    AckMap = maps:new(),

    {Bottom, DeltaBuffer, AckMap}.

%% @private
min_seq_ack_map(AckMap) ->
    lists:min(maps:values(AckMap)).

%% @private
last_ack(NodeName, AckMap) ->
    maps:get(NodeName, AckMap, 0).
