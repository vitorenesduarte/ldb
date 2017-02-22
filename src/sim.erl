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

-module(sim).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% sim callbacks
-export([start_link/0,
         create/3,
         show/2,
         show/3,
         update/3,
         sync/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {nodes}).

-type sim_node() :: atom().
-type sim_nodes() :: sim_node() | list(sim_nodes()).

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(sim_nodes(), key(), type()) -> ok.
create(Nodes, Key, Type) ->
    gen_server:call(?MODULE, {create, Nodes, Key, Type}).

-spec show(sim_nodes(), key()) -> ok.
show(Nodes, Key) ->
    show(Nodes, Key, false).

-spec show(sim_nodes(), key(), boolean()) -> ok.
show(Nodes, Key, ShowCRDT) ->
    gen_server:call(?MODULE, {show, Nodes, Key, ShowCRDT}).

-spec update(sim_nodes(), key(), operation()) -> ok.
update(Nodes, Key, Op) ->
    gen_server:call(?MODULE, {update, Nodes, Key, parse_op(Op)}).

-spec sync(sim_nodes(), key()) -> ok.
sync(Nodes, Key) ->
    gen_server:call(?MODULE, {sync, Nodes, Key}).

%% gen_server callbacks
init([]) ->
    ldb_log:info("sim initialized!"),
    {ok, #state{nodes=ordsets:new()}}.

handle_call({create, Nodes, Key, Type}, _From, State) when is_list(Nodes) ->
    handle_create(Nodes, Key, Type, State);

handle_call({create, Node, Key, Type}, _From, State) ->
    handle_create([Node], Key, Type, State);

handle_call({show, Nodes, Key, ShowCRDT}, _From, State) when is_list(Nodes) ->
    handle_show(Nodes, Key, ShowCRDT, State);

handle_call({show, Node, Key, ShowCRDT}, _From, State) ->
    handle_show([Node], Key, ShowCRDT, State);

handle_call({update, Nodes, Key, Op}, _From, State) when is_list(Nodes) ->
    handle_update(Nodes, Key, Op, State);

handle_call({update, Node, Key, Op}, _From, State) ->
    handle_update([Node], Key, Op, State);

handle_call({sync, Nodes, Key}, _From, State) when is_list(Nodes) ->
    handle_sync(Nodes, Key, State);

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
handle_create(Nodes, Key, Type, #state{nodes=Connected0}) ->
    lists:foreach(
        fun(Node) ->
            ldb:create({Node, Key}, Type)
        end,
        Nodes
    ),
    Connected1 = ordsets:union(Connected0, ordsets:from_list(Nodes)),
    {reply, ok, #state{nodes=Connected1}}.

handle_show(Nodes, Key, ShowCRDT, #state{nodes=Connected}=State) ->
    lists:foreach(
        fun(Node) ->
            case ordsets:is_element(Node, Connected) of
                true ->
                    case ldb:query({Node, Key}) of
                        {ok, Value0} ->
                            {ok, {Type, CRDT}} = ldb_store:get({Node, Key}),
                            Value = case Type == state_mvregister orelse Type == state_awset of
                                true ->
                                    sets:to_list(Value0);
                                false ->
                                    Value0
                            end,
                            case ShowCRDT of
                                true ->
                                    io:format("~p:~nValue: ~p~nState: ~p~n", [Node, Value, CRDT]);
                                false ->
                                    io:format("~p:~nValue: ~p~n", [Node, Value])
                            end;
                        {error, not_found} ->
                            io:format("Key ~p not found for node ~p.~n", [Key, Node])
                    end;
                false ->
                    io:format("Node ~p is not connected.~n", [Node])
            end
        end,
        Nodes
    ),
    {reply, ok, State}.

handle_update(Nodes, Key, Op, #state{nodes=Connected}=State) ->
    lists:foreach(
        fun(Node) ->
            case ordsets:is_element(Node, Connected) of
                true ->
                    case ldb:update(Node, {Node, Key}, Op) of
                        ok ->
                            ok;
                        {error, not_found} ->
                            io:format("Key ~p not found for node ~p.~n", [Key, Node]);
                        Error ->
                            io:format("Error updating key ~p for node ~p: ~p.~n", [Key, Node, Error])
                    end;
                false ->
                    io:format("Node ~p is not connected.~n", [Node])
            end
        end,
        Nodes
    ),
    {reply, ok, State}.

handle_sync(Nodes, Key, #state{nodes=Connected}=State) ->
    {CRDT, Valid} = lists:foldl(
        fun(Node, {CRDT0, Valid0}=Acc) ->
            case ordsets:is_element(Node, Connected) of
                true ->
                    case ldb_store:get({Node, Key}) of
                        {ok, {Type, _}=LocalCRDT} ->
                            CRDT1 = case CRDT0 of
                                undefined ->
                                    LocalCRDT;
                                _ ->
                                    Type:merge(CRDT0, LocalCRDT)
                            end,
                            {CRDT1, ordsets:add_element(Node, Valid0)};
                        {error, not_found} ->
                            io:format("Key ~p not found for node ~p.~n", [Key, Node]),
                            Acc
                    end;
                false ->
                    io:format("Node ~p is not connected.~n", [Node]),
                    Acc
            end
        end,
        {undefined, ordsets:new()},
        Nodes
    ),

    ReplaceFun = fun(_) -> {ok, CRDT} end,
    lists:foreach(
        fun(Node) ->
            ldb_store:update({Node, Key}, ReplaceFun)
        end,
        Valid
    ),

    {reply, ok, State}.

%% @private
parse_op({set, V}) ->
    {set, undefined, V};
parse_op(Op) ->
    Op.
