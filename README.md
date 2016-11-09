# ldb (maybe rename to CRDT-Bench): A benchmarking platform for CRDT different models.

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)


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
2> Port = 1789.
3> ldb_pull_logs:go(PublicHost, Port).
```

