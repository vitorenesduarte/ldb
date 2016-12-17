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

%% space server
-define(SPACE_TCP_OPTIONS, [list, {packet, line}]).

%% pure_op
-type vclock() :: orddict:orddict().
-type mclock() :: orddict:orddict().

%% defaults
-define(DEFAULT_MODE, state_based).
-define(DEFAULT_STORE, ldb_ets_store).
