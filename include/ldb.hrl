-type error() :: {error, atom()}.

%% ldb
-type key() :: string().
-type type() :: term().
-type value() :: term().
-type operation() :: term().

%% peer service
-type specs() :: {node(), inet:ip_address(), non_neg_integer()}.
-type message() :: term().
