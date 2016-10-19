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
         message_maker/0,
         message_handler/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: atom()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(key(), type()) -> ok | already_exists().
create(Key, Type) ->
    gen_server:call(?MODULE, {create, Key, Type}, infinity).

-spec query(key()) -> {ok, value()} | not_found().
query(Key) ->
    gen_server:call(?MODULE, {query, Key}, infinity).

-spec update(key(), operation()) -> ok | not_found() | error().
update(Key, Operation) ->
    gen_server:call(?MODULE, {update, Key, Operation}, infinity).

-spec message_maker() -> function().
message_maker() ->
    fun(Key, {{Type, _}=CRDT, Sequence, DeltaBuffer, AckMap}, NodeName) ->
        MinSeq = min_seq(DeltaBuffer),
        LastAck = last_ack(NodeName, AckMap),

        case LastAck < Sequence of
            true ->
                Delta = case orddict:is_empty(DeltaBuffer) orelse MinSeq > LastAck of
                    true ->
                        CRDT;
                    false ->
                        orddict:fold(
                            fun(N, {From, D}, Acc) ->
                                case LastAck =< N andalso N < Sequence andalso
                                     NodeName =/= From  of
                                    true ->
                                        Type:merge(Acc, D);
                                    false ->
                                        Acc
                                end
                            end,
                            %% @todo support complex types
                            Type:new(),
                            DeltaBuffer
                        )
                end,
                ldb_log:info("LALALA delta size ~p | crdt size ~p", [byte_size(term_to_binary(Delta)), byte_size(term_to_binary(CRDT))]),

                Message = {Key, delta_send, node(), Sequence, Delta},
                {ok, Message};
            false ->
                nothing
        end
    end.

-spec message_handler(term()) -> function().
message_handler({_, delta_send, _, _, _}) ->
    fun({Key, delta_send, From, N, {Type, _}=RemoteCRDT}) ->
        %% Create a bottom entry (if not present)
        %% @todo support complex types
        _ = ldb_store:create(Key, {Type:new(), 0, orddict:new(), orddict:new()}),
        ldb_store:update(
            Key,
            fun({LocalCRDT, Sequence0, DeltaBuffer0, AckMap}) ->
                Merged = Type:merge(LocalCRDT, RemoteCRDT),

                {Sequence, DeltaBuffer} = case join_decompositions() of
                    true ->
                        Delta = Type:delta(state_driven, RemoteCRDT, LocalCRDT),

                        %% If what we received, inflates the local state
                        case not Type:is_bottom(Delta) of
                            true ->
                                DeltaBuffer1 = orddict:store(Sequence0, {From, Delta}, DeltaBuffer0),
                                Sequence1 = Sequence0 + 1,
                                {Sequence1, DeltaBuffer1};
                            false ->
                                {Sequence0, DeltaBuffer0}
                        end;
                    false ->

                        %% If what we received, inflates the local state
                        case Type:is_strict_inflation(LocalCRDT, Merged) of
                            true ->
                                DeltaBuffer1 = orddict:store(Sequence0, {From, RemoteCRDT}, DeltaBuffer0),
                                Sequence1 = Sequence0 + 1,
                                {Sequence1, DeltaBuffer1};
                            false ->
                                {Sequence0, DeltaBuffer0}
                        end
                end,

                send_ack(From, {Key, delta_ack, node(), N}),

                StoreValue = {Merged, Sequence, DeltaBuffer, AckMap},
                {ok, StoreValue}
            end
        )
    end;
message_handler({_, delta_ack, _, _}) ->
    fun({Key, delta_ack, From, N}) ->
        ldb_store:update(
            Key,
            fun({LocalCRDT, Sequence, DeltaBuffer, AckMap0}) ->
                LastAck = last_ack(From, AckMap0),
                MaxAck = max(LastAck, N),
                AckMap1 = orddict:store(From, MaxAck, AckMap0),
                StoreValue = {LocalCRDT, Sequence, DeltaBuffer, AckMap1},
                {ok, StoreValue}
            end
        )
    end.

%% @private
min_seq(DeltaBuffer) ->
    case orddict:fetch_keys(DeltaBuffer) of
        [] ->
            0;
        Keys ->
            lists:nth(1, Keys)
    end.

%% @private
last_ack(NodeName, AckMap) ->
    case orddict:find(NodeName, AckMap) of
        {ok, Ack} ->
            Ack;
        _ ->
            0
    end.

%% @private
send_ack(NodeName, AckMessage) ->
    ldb_whisperer:send(NodeName, AckMessage).

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = node(),

    ldb_log:info("ldb_delta_based_backend initialized!", extended),
    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    Type = ldb_util:get_type(LDBType),

    %% @todo support complex types
    CRDT = Type:new(),
    Sequence = 0,
    DeltaBuffer = orddict:new(),
    AckMap = orddict:new(),

    StoreValue = {CRDT, Sequence, DeltaBuffer, AckMap},
    Result = ldb_store:create(Key, StoreValue),
    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {{Type, _}=CRDT, _, _, _}} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update, Key, Operation}, _From, #state{actor=Actor}=State) ->
    Function = fun({{Type, _}=CRDT0, Sequence, DeltaBuffer0, AckMap}) ->
        case Type:delta_mutate(Operation, Actor, CRDT0) of
            {ok, Delta} ->
                CRDT1 = Type:merge(CRDT0, Delta),
                DeltaBuffer1 = orddict:store(Sequence, {Actor, Delta}, DeltaBuffer0),
                StoreValue = {CRDT1, Sequence + 1, DeltaBuffer1, AckMap},
                {ok, StoreValue};
            Error ->
                Error
        end
    end,

    Result = case ldb_store:update(Key, Function) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end,
    {reply, Result, State};

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
join_decompositions() ->
    application:get_env(?APP,
                        ldb_join_decompositions,
                        false).
