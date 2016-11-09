# ldb

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- pure-op-based (to be merged [#6](https://github.com/vitorenesduarte/ldb/pull/6))

Next features:
- implement Delta-state-based replication (as in [http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf](http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf))
- implement new replication model only using join-decompositions ([http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf))

#### Docker
To build an image:

```bash
$ cd Dockerfiles/
$ docker build -t vitorenesduarte/ldb -f ldb  .
```

To push it to [Docker Hub](https://hub.docker.com/):

```bash
$ docker push vitorenesduarte/ldb
```

## Remote experiments in DCOS
First authenticate to the DCOS cluster with:

```bash
$ dcos auth login
```

Once authenticated, launch the Mongo instance
(this is where LDBs instance will push their logs).

```bash
$ cd bin/
$ ./mongo-deploy.sh
```

Last but not least, launch LDBs instances.
```bash
$ cd bin/
$ ./ldb-deploy.sh
```

In order to execute this last script, a set of environment variables
have to be defined:

- __BRANCH__: which ldb branch should LDBs instances run
- __LDB_MODE__:
  - state_based
  - delta_based
  - pure_op_based
- __LDB_JOIN_DECOMPOSITIONS__: when set to _true_, applies
join-decompositions to the received delta buffers (this will only
have an effect if __LBD_MODE=delta_based__)
- __LDB_DCOS_OVERLAY__:
  - line
  - ring
  - hyparview
  - erdos_renyi
- __LDB_SIMULATION__:
  - basic
- __LDB_NODE_NUMBER__: number of LDBs nodes. Since the overlays are
not yet created in runtime, only __3__ and __13__ nodes are supported
for now
- __LDB_EVALUATION_IDENTIFIER__: set this with one of the following
values, depending on which evaluation you are running. This id will
be later used to generate the graphs with proper labels.
  - state_based_$LDB_DCOS_OVERLAY
  - delta_based_$LDB_DCOS_OVERLAY
  - pure_op_based_$LDB_DCOS_OVERLAY
  - join_decompositions_$LDB_DCOS_OVERLAY
- __LDB_EVALUATION_TIMESTAMP__: When running concurrent experiments
in the cluster, this timestamp should be unique.
- __LDB_INSTRUMENTATION__: this should be set to _true_ if logs are to
be pushed to the Mongo instance
- __LDB_EXTENDED_LOGGING__: _true_/_false_


To see the results of the experiments, firstly you need to pull logs
from Mongo instance.

- Find Mongo and __private host__ and __port__ with

```bash
$ bin/get-mongo-config.sh
```

- Find the respective __public host__ and

```bash
$ make shell
1> PublicHost = "192.168.116.148".
"192.168.116.148"
2> Port = 1756.
1756
3> ldb_pull_logs:go(PublicHost, Port).
ok
4>
```

