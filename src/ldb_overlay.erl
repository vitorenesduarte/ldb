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

-module(ldb_overlay).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com").

-include("ldb.hrl").

-export([get/2]).

%% @doc The first argument can be:
%%          - `line'
%%          - `ring'
%%          - `hyparview'
%%          - `erdos_renyi'
%%      The second argument is the number of nodes.
-spec get(atom(), non_neg_integer()) -> orddict:orddict().
get(_, 1) ->
    [];
get(line, 3) ->
    [{0, [1]},
     {1, [0, 2]},
     {2, [1]}];
get(line, 13) ->
    [{0, [1]},
     {1, [0, 2]},
     {2, [1, 3]},
     {3, [2, 4]},
     {4, [3, 5]},
     {5, [4, 6]},
     {6, [5, 7]},
     {7, [6, 8]},
     {8, [7, 9]},
     {9, [8, 10]},
     {10, [9, 11]},
     {11, [10, 12]},
     {12, [11]}];
get(ring, 3) ->
    [{0, [2, 1]},
     {1, [0, 2]},
     {2, [1, 0]}];
get(ring, 5) ->
    [{0, [4, 1]},
     {1, [0, 2]},
     {2, [1, 3]},
     {3, [2, 4]},
     {4, [3, 0]}];
get(ring, 13) ->
    [{0, [12, 1]},
     {1, [0, 2]},
     {2, [1, 3]},
     {3, [2, 4]},
     {4, [3, 5]},
     {5, [4, 6]},
     {6, [5, 7]},
     {7, [6, 8]},
     {8, [7, 9]},
     {9, [8, 10]},
     {10, [9, 11]},
     {11, [10, 12]},
     {12, [11, 0]}];
get(hyparview, 13) ->
    [{0, [1, 2, 11]},
     {1, [0, 2, 3]},
     {2, [0, 1, 7]},
     {3, [1, 5, 8]},
     {4, [6, 7, 8]},
     {5, [3, 9, 12]},
     {6, [4, 7, 12]},
     {7, [2, 4, 6]},
     {8, [3, 4, 10]},
     {9, [5, 11]},
     {10, [8, 11, 12]},
     {11, [0, 9, 10]},
     {12, [5, 6, 10]}];
get(erdos_renyi, 13) ->
    [{0, [4, 6, 10]},
     {1, [2, 5, 8, 9, 12]},
     {2, [1, 3, 7, 9, 11, 12]},
     {3, [2, 8, 6]},
     {4, [0, 9]},
     {5, [1, 10, 11, 12]},
     {6, [0, 3, 11]},
     {7, [2, 9]},
     {8, [1, 3, 9]},
     {9, [1, 2, 4, 7, 8, 10, 11]},
     {10, [0, 5, 9]},
     {11, [2, 5, 6, 9]},
     {12, [1, 2, 5]}].
