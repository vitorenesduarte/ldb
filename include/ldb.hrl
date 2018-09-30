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

%% size metric: metadata size, payload size
-type size_metric() :: {non_neg_integer(), non_neg_integer()}.

%% clocks
-type sequence() :: non_neg_integer().
-type dot() :: {ldb_node_id(), sequence()}.
-type vclock() :: vclock:v().
-type m_vclock() :: m_vclock:m().

%% space server
-define(SPACE_TCP_OPTIONS, [list, {packet, line}]).

%% defaults
-define(DEFAULT_STATE_SYNC_INTERVAL, 1000).
-define(DEFAULT_EVICTION_ROUND_NUMBER, -1). %% don't perform peer eviction
-define(DEFAULT_MODE, state_based).
-define(DEFAULT_DRIVEN_MODE, none).
-define(DEFAULT_STORE, ldb_actor_store).

%% logging
-ifdef(debug).
-define(DEBUG(M), lager:info(M)).
-define(DEBUG(M, A), lager:info(M, A)).
-else.
-define(DEBUG(_M), ok).
-define(DEBUG(_M, _A), ok).
-endif.
