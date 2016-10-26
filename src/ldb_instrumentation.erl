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

-module(ldb_instrumentation).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-behaviour(gen_server).

%% ldb_instrumentation callbacks
-export([start_link/0,
         transmission/2,
         convergence/0,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {tref :: timer:tref(),
                size_per_type :: orddict:orddict()}).

-define(TRANSMISSION_INTERVAL, 1000). %% 1 second.

-spec start_link()-> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec transmission(term(), term()) -> ok.
transmission(Type, Payload) ->
    gen_server:cast(?MODULE, {transmission, Type, Payload}).

-spec convergence() -> ok.
convergence() ->
    gen_server:call(?MODULE, convergence, infinity).

-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

%% gen_server callbacks
init([]) ->
    LogDir = log_dir(),
    filelib:ensure_dir(LogDir),

    Filename = main_log(),
    Line = io_lib:format("Type,Seconds,MegaBytes\n", []),
    write_to_file(Filename, Line),

    {ok, TRef} = start_transmission_timer(),

    ldb_log:info("Instrumentation timer enabled!", extended),
    {ok, #state{tref=TRef, size_per_type=orddict:new()}}.

handle_call(convergence, _From, State) ->
    record_convergence(),
    {reply, ok, State};

handle_call(stop, _From, #state{tref=TRef}=State) ->
    {ok, cancel} = timer:cancel(TRef),
    ldb_log:info("Instrumentation timer disabled!", extended),
    {reply, ok, State#state{tref=undefined}};

handle_call(Msg, _From, State) ->
    ldb_log:warning("Unhandled call message: ~p", [Msg]),
    {noreply, State}.

handle_cast({transmission, Type, Payload}, #state{size_per_type=Map0}=State) ->
    TransmissionType = get_transmission_type(Type),
    Size = termsize(Payload),
    Current = case orddict:find(TransmissionType, Map0) of
        {ok, Value} ->
            Value;
        error ->
            0
    end,
    Map = orddict:store(TransmissionType, Current + Size, Map0),
    {noreply, State#state{size_per_type=Map}};

handle_cast(Msg, State) ->
    ldb_log:warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(transmission, #state{size_per_type=Map}=State) ->
    {ok, TRef} = start_transmission_timer(),
    record_transmission(Map),
    {noreply, State#state{tref=TRef}};

handle_info(Msg, State) ->
    ldb_log:warning("Unhandled info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
termsize(Term) ->
    byte_size(term_to_binary(Term)).

%% @private
start_transmission_timer() ->
    timer:send_after(?TRANSMISSION_INTERVAL, transmission).

%% @private
root_eval_dir() ->
    code:priv_dir(?APP) ++ "/evaluation".

%% @private
root_log_dir() ->
    root_eval_dir() ++ "/logs".

%% @private
log_dir() ->
    root_log_dir() ++ "/" ++ simulation_id() ++ "/".

%% @private
simulation_id() ->
    {ok, Simulation} = application:get_env(?APP,
                                           ldb_simulation),
    LocalOrDCOS = case os:getenv("DCOS", "undefined") of
        "undefined" ->
            "local";
        _ ->
            "dcos"
    end,
    {ok, EvalIdentifier} = application:get_env(?APP,
                                              ldb_evaluation_identifier),
    {ok, EvalTimestamp} = application:get_env(?APP,
                                              ldb_evaluation_timestamp),

    Id = atom_to_list(Simulation) ++ "/"
      ++ LocalOrDCOS ++ "/"
      ++ atom_to_list(EvalIdentifier) ++ "/"
      ++ integer_to_list(EvalTimestamp),
    Id.

%% @private
main_log() ->
    log_dir() ++ main_log_suffix().

%% @private
main_log_suffix() ->
    atom_to_list(node()) ++ ".csv".

%% @private
megasize(Size) ->
    KiloSize = Size / 1024,
    MegaSize = KiloSize / 1024,
    MegaSize.

%% @private
record_transmission(Map) ->
    Filename = main_log(),
    Timestamp = ldb_util:timestamp(),
    Lines = orddict:fold(
        fun(Type, Size, Acc) ->
            Acc ++ get_line(Type, Timestamp, Size)
        end,
        "",
        Map
    ),
    append_to_file(Filename, Lines).

%% @private
record_convergence() ->
    Filename = main_log(),
    Timestamp = ldb_util:timestamp(),
    Line = get_line(convergence, Timestamp, 0),
    append_to_file(Filename, Line).

%% @private
get_line(Type, Timestamp, Size) ->
    io_lib:format(
        "~w,~w,~w\n",
        [Type, Timestamp, megasize(Size)]
    ).

%% @private
write_to_file(Filename, Line) ->
    write_file(Filename, Line, write).

%% @private
append_to_file(Filename, Line) ->
    write_file(Filename, Line, append).

%% @private
write_file(Filename, Line, Mode) ->
    ok = file:write_file(Filename, Line, [Mode]),
    ok.

%% @private
get_transmission_type(state_send) -> state_send;
get_transmission_type(delta_send) -> delta_send;
get_transmission_type(delta_ack) -> delta_send;
get_transmission_type(tcbcast) -> pure_send;
get_transmission_type(tcbcast_ack) -> pure_send;
get_transmission_type(tcbdeliver) -> pure_send.
