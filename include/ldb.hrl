-type not_found() :: {error, not_found}.
-type error() :: {error, atom()}.

%% ldb
-type key() :: string().
-type type() :: term().
-type value() :: term().
-type operation() :: term().

%% peer service
-type specs() :: {node(), inet:ip_address(), non_neg_integer()}.
-type message() :: term().
-define(PEER_IP, {127,0,0,1}).
-define(PEER_PORT, 9000).
