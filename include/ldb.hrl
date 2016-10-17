-define(APP, ldb).
-type not_found() :: {error, not_found}.
-type error() :: {error, atom()}.

%% ldb
-type key() :: string().
-type type() :: term().
-type value() :: term().
-type operation() :: term().

%% peer service
-type node_info() :: {node(), inet:ip_address(), non_neg_integer()}.
-type handler() :: {term(), term()}. %% {module, function}
-type message() :: term().
-define(TCP_OPTIONS, [binary, {active, true}, {packet, 4}, {keepalive, true}]).

%% defaults
-define(DEFAULT_MODE, state_based).
-define(DEFAULT_STORE, ldb_ets_store).
-define(DEFAULT_PEER_SERVICE, ldb_static_peer_service).
