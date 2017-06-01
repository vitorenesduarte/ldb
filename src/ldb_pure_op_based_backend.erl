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

-module(ldb_pure_op_based_backend).
-author("Georges Younes <georges.r.younes@gmail.com").

-include("ldb.hrl").

-define(ISHIKAWA, ishikawa).

-behaviour(ldb_backend).
-behaviour(gen_server).

%% ldb_backend callbacks
-export([start_link/0,
         create/2,
         query/1,
         update/2,
         message_maker/0,
         message_handler/1,
         memory/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {actor :: ldb_node_id()}).

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

-spec message_maker() -> function().
message_maker() ->
    fun(_, _, _) ->
        lager:warning("message_maker called and it was not supposed to")
    end.

-spec message_handler(term()) -> function().
message_handler(_) ->
    fun(M) ->
        gen_server:cast(?MODULE, M)
    end.

-spec memory() -> {non_neg_integer(), non_neg_integer()}.
memory() ->
    gen_server:call(?MODULE, memory, infinity).

%% @todo do spec
delivery_function({VV, {Key, EncodedOp}}) ->
    ?LOG("message delivered ~p~n~n", [{VV, {Key, EncodedOp}}]),
    Operation = decode_op(EncodedOp),
    Function = fun({Type, _}=CRDT) ->
        Type:mutate(Operation, VV, CRDT)
    end,

    _Result = ldb_store:update(Key, Function),
    ok.

%% gen_server callbacks
init([]) ->
    {ok, _Pid} = ldb_store:start_link(),
    Actor = ldb_config:id(),

    ?ISHIKAWA:tcbdelivery(fun(Msg) -> delivery_function(Msg) end),

    case ldb_config:get(lmetrics) of
        true ->
            lmetrics:set_time_series_callback(fun() -> ToBeAdded = memory(), {ok, ToBeAdded} end);
        false ->
            ok
    end,

    ?LOG("ldb_pure_op_based_backend initialized!"),

    {ok, #state{actor=Actor}}.

handle_call({create, Key, LDBType}, _From, State) ->
    Bottom = ldb_util:new_crdt(type, LDBType),
    Result = ldb_store:create(Key, Bottom),
    {reply, Result, State};

handle_call({query, Key}, _From, State) ->
    Result = case ldb_store:get(Key) of
        {ok, {Type, _}=CRDT} ->
            {ok, Type:query(CRDT)};
        Error ->
            Error
    end,
    {reply, Result, State};

handle_call({update, Key, Operation}, _From, State) ->
    %% Generate message.
    MessageBody = {Key, encode_op(Operation)},

    %% broadcast
    {ok, VV} = ?ISHIKAWA:tcbcast(MessageBody),

    Function = fun({Type, _}=CRDT) ->
        Type:mutate(Operation, VV, CRDT)
    end,

    Result = ldb_store:update(Key, Function),
    {reply, Result, State};

handle_call(memory, _From, State) ->
    FoldFunction = fun({_Key, CRDT}, C) ->
        Size = ldb_util:size(crdt, CRDT),
        C + Size
    end,

    CRDTSize = ldb_store:fold(FoldFunction, 0),

    CalcFunction = fun({VV, SVV, RTM, ToBeAckQueue, ToBeDeliveredQueue}) ->
        erts_debug:flat_size(VV)
        + erts_debug:flat_size(SVV)
        + erts_debug:flat_size(RTM)
        + erts_debug:flat_size(ToBeAckQueue)
        + erts_debug:flat_size(ToBeDeliveredQueue)
    end,

    TRCBSize = ?ISHIKAWA:tcbmemory(CalcFunction),

    {reply, {CRDTSize, TRCBSize} , State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

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
encode_op({add, E}) ->
    {1, E};
%% @todo
encode_op(Op) ->
    Op.

%% @private
decode_op({1, E}) ->
    {add, E};
%% @todo
decode_op(Op) ->
    Op.