-define(APP, ldb).
-type already_exists() :: {error, already_exists}.
-type not_found() :: {error, not_found}.
-type error() :: {error, atom()}.

%% ldb
-type key() :: string().
-type type() :: term().
-type value() :: term().
-type operation() :: term().

%% peer service
-type node_name() :: node().
-type node_ip() :: inet:ip_address().
-type node_port() :: non_neg_integer().
-type node_info() :: {node_name(), node_ip(), node_port()}.
-type handler() :: {term(), term()}. %% {module, function}
-type message() :: term().
-define(TCP_OPTIONS, [binary, {active, true}, {packet, 4}, {keepalive, true}]).

%% defaults
-define(DEFAULT_MODE, state_based).
-define(DEFAULT_STORE, ldb_ets_store).
-define(DEFAULT_PEER_SERVICE, ldb_static_peer_service).
