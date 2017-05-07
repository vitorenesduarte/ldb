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
         keys/0,
         get/1,
         create/2,
         update/2,
         update_all/1,
         fold/2]).

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

-spec keys() -> list(key()).
keys() ->
    gen_server:call(?MODULE, keys, infinity).

-spec get(key()) -> {ok, value()} | not_found().
get(Key) ->
    gen_server:call(?MODULE, {get, Key}, infinity).

-spec create(key(), value()) -> ok.
create(Key, Value) ->
    gen_server:call(?MODULE, {create, Key, Value}, infinity).

-spec update(key(), function()) -> ok | not_found() | error().
update(Key, Function) ->
    gen_server:call(?MODULE, {update, Key, Function}, infinity).

-spec update_all(function()) -> ok.
update_all(Function) ->
    gen_server:call(?MODULE, {update_all, Function}, infinity).

-spec fold(function(), term()) -> term().
fold(Function, Acc) ->
    gen_server:call(?MODULE, {fold, Function, Acc}, infinity).

%% gen_server callbacks
init([]) ->
    ETS = ets:new(node(), [ordered_set, private]),

    ?LOG("ldb_ets_store initialized!"),
    {ok, #state{ets_id=ETS}}.

handle_call(keys, _From, #state{ets_id=ETS}=State) ->
    Result = keys(ETS),
    {reply, Result, State};

handle_call({get, Key}, _From, #state{ets_id=ETS}=State) ->
    Result = do_get(Key, ETS),
    {reply, Result, State};

handle_call({create, Key, Value}, _From, #state{ets_id=ETS}=State) ->
    Result = case do_get(Key, ETS) of
        {ok, _} ->
            ok;
        _ ->
            do_put(Key, Value, ETS),
            ok
    end,

    {reply, Result, State};

handle_call({update, Key, Function}, _From, #state{ets_id=ETS}=State) ->
    Result = case do_get(Key, ETS) of
        {ok, Value} ->
            case Function(Value) of
                {ok, NewValue} ->
                    do_put(Key, NewValue, ETS),
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end,

    {reply, Result, State};

handle_call({update_all, Function}, _From, #state{ets_id=ETS}=State) ->
    Keys = keys(ETS),

    lists:foreach(
        fun(Key) ->
            {ok, Value} = do_get(Key, ETS),
            case Function(Value) of
                {ok, NewValue} ->
                    do_put(Key, NewValue, ETS);
                _ ->
                    ok
            end
        end,
        Keys
    ),

    Result = ok,

    {reply, Result,  State};

handle_call({fold, Function, Acc}, _From, #state{ets_id=ETS}=State) ->
    Result = ets:foldl(Function, Acc, ETS),
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
do_get(Key, ETS) ->
    case ets:lookup(ETS, Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, not_found}
    end.

%% @private Inserts a value in the store (replacing if exists)
do_put(Key, Value, ETS) ->
    true = ets:insert(ETS, {Key, Value}),
    ok.

%% @private Get list of current keys.
keys(ETS) ->
    keys(ets:first(ETS), ETS, []).

keys('$end_of_table', _ETS, Acc) ->
    Acc;
keys(Key, ETS, Acc) ->
    keys(ets:next(ETS, Key), ETS, [Key | Acc]).
