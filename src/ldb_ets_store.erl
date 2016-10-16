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

-module(ldb_ets_store).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(ldb_store).
-behaviour(gen_server).

%% ldb_store callbacks
-export([start_link/0,
         get/1,
         put/2,
         update/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {ets_id :: term()}).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get(key()) -> {ok, value()} | not_found().
get(Key) ->
    gen_server:call(?MODULE, {get, Key}, infinity).

-spec put(key(), value()) -> ok.
put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}, infinity).

-spec update(key(), function()) ->
    {ok, value()} | not_found() | error().
update(Key, Function) ->
    gen_server:call(?MODULE, {update, Key, Function}, infinity).

%% gen_server callbacks
init([]) ->
    ETS = ets:new(node(), [ordered_set, private]),

    lager:info("ldb_ets_store initialized!"),
    {ok, #state{ets_id=ETS}}.

handle_call({get, Id}, _From, #state{ets_id=ETS}=State) ->
    Result = do_get(Id, ETS),
    {reply, Result, State};

handle_call({put, Id, Value}, _From, #state{ets_id=ETS}=State) ->
    do_put(Id, Value, ETS),
    {reply, ok, State};

handle_call({update, Id, Function}, _From, #state{ets_id=ETS}=State) ->
    Result = case do_get(Id, ETS) of
        {ok, Value} ->
            case Function(Value) of
                {ok, NewValue} ->
                    do_put(Id, NewValue, ETS),
                    {ok, NewValue};
                Error ->
                    Error
            end;
        Error ->
            Error
    end,
    {reply, Result, State};

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

%% @private Attemts to retrieve a certain value from the ets.
do_get(Id, ETS) ->
    case ets:lookup(ETS, Id) of
        [{Id, Value}] ->
            {ok, Value};
        [] ->
            {error, not_found}
    end.

do_put(Id, Value, ETS) ->
    true = ets:insert(ETS, {Id, Value}),
    ok.
