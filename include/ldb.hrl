-define(APP, ldb).
-type not_found() :: {error, not_found}.
-type error() :: {error, atom()}.

%% ldb
-type key() :: string().
-type type() :: term().
-type value() :: term().
-type operation() :: term().

%% peer service
-type ldb_node_id() :: node().
-type node_ip() :: inet:ip_address().
-type node_port() :: non_neg_integer().
-type node_spec() :: {ldb_node_id(), node_ip(), node_port()}.
-type handler() :: term(). %% module
-type message() :: term().
-type timestamp() :: non_neg_integer().

%% space server
-define(SPACE_TCP_OPTIONS, [list, {packet, line}]).

%% pure_op
-type vclock() :: orddict:orddict().
-type mclock() :: orddict:orddict().

%% defaults
-define(DEFAULT_STATE_SYNC_INTERVAL, 1000).
-define(DEFAULT_EVICTION_ROUND_NUMBER, -1). %% don't perform peer eviction
-define(DEFAULT_MODE, state_based).
-define(DEFAULT_DRIVEN_MODE, none).
-define(DEFAULT_STORE, ldb_ets_store).
-define(METRICS_DEFAULT, true).

%% logging
-define(LOGGING, list_to_atom("true")). %% dialyzer
-define(LOG(S),
        ?LOG(S, [])
       ).
-define(LOG(S, Args),
        case ?LOGGING of
            true ->
                lager:info(S, Args);
            false ->
                ok
        end
       ).
