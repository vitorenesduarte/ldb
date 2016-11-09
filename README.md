# ldb

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- pure-op-based (to be merged https://github.com/vitorenesduarte/ldb/pull/6)

Next features:
- implement Delta-state-based replication (as in [http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf](http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf)
- implement new replication model only using join-decompositions ([http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf)

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



#### Pull logs from Mongo instance running in Marathon

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

