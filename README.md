# ldb

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- pure-op-based

Next features:
- implement Delta-state-based replication (as in [http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf](http://novasys.di.fct.unl.pt/~alinde/publications/a12-van_der_linde.pdf))
- implement new replication model only using join-decompositions ([http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf))


### Configuration
- __LDB_MODE__:
  - state_based
  - delta_based
  - pure_op_based
- __LDB_DRIVEN_MODE__:
  - none
  - state_driven
  - digest_driven
- __LDB_REDUNDANT_DGROUPS__: when set to _true_,
removes redundant state that may be present in the received
delta-groups, using [join-decompositions](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf)
- __LDB_DGROUP_BACK_PROPAGATION__: when set to _true_,
avoids back-propagation of delta-groups
- __LDB_METRICS__: metrics are recorded if _true_

| __LDB_MODE__ | __LDB_DRIVEN_MODE__ | __LDB_REDUNDANT_DGROUPS__ | __LDB_DGROUP_BACK_PROPAGATION__ |
|:------------:|:-------------------:|:-------------------------:|:--------------------------------|
| state_based  | none                | N/A                       | N/A                             |
| state_based  | state_driven        | N/A                       | N/A                             |
| state_based  | digest_driven       | N/A                       | N/A                             |
| delta_based  | none                | boolean                   | boolean                         |
| delta_based  | state_driven        | boolean                   | boolean                         |
| delta_based  | digest_driven       | boolean                   | boolean                         |
| pure_op_based| N/A                 | N/A                       | N/A                             |
