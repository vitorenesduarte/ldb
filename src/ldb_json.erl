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

-module(ldb_json).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

%% ldb_json callbacks
-export([encode/1,
         decode/1]).

%% @doc
-spec encode(term()) -> binary().
encode(D) ->
    jsx:encode(D).

%% @doc
decode(E) when is_list(E) ->
    decode(list_to_binary(E));
decode(E) when is_binary(E) ->
    Opts = [{labels, atom}],
    Parsed = parse(jsx:decode(E, Opts)),
    maps:from_list(Parsed).

%% @private
parse(A) when is_atom(A) ->
    A;
parse(N) when is_number(N) ->
    N;
parse(B) when is_binary(B) ->
    binary_to_list(B);
parse({K, V}) ->
    {K, parse(V)};
parse([H|T]) ->
    [parse(H)|parse(T)].
