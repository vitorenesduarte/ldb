# ldb 

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- dotted-based (scuttebutt)

### Configuration
- __LDB_MODE__:
  - `state_based`
  - `delta_based`
  - `scuttlebutt`
- __LDB_STATE_SYNC_INTERVAL__: state is propagated every `X` milliseconds
- __LDB_REDUNDANT_DGROUPS__: when set to `true`,
removes redundant state that may be present in the received
delta-groups, using [join-decompositions](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf)
- __LDB_DGROUP_BACK_PROPAGATION__: when set to `true`,
avoids back-propagation of delta-groups
- __LDB_METRICS__: metrics are recorded if `true`

State-based and delta-based modes are described [here](http://vitorenesduarte.github.io/page/other/msc-thesis.pdf),
as well as the optimizations removal of redundant delta-groups and avoiding back-propagation of delta-groups.

|||||
|---------------------------------|-------------------------------------------|-------------------------------------------|---------------|
| __NODE_NUMBER__                 | `0..`                                     | `0..`                                     | `0..`
| __LDB_MODE__                    | `state_based`                             | `delta_based`                             | `scuttlebutt` |
| __LDB_STATE_SYNC_INTERVAL__     | `0..`                                     | `0..`                                     | `0..`
| __LDB_REDUNDANT_DGROUPS__       | __NA__                                    | `true` / `false`                          | __NA__        |
| __LDB_DGROUP_BACK_PROPAGATION__ | __NA__                                    | `true` / `false`                          | __NA__        |

#### Defaults
- __LDB_MODE__: `state_based`
- __LDB_STATE_SYNC_INTERVAL__: 1000
- __LDB_REDUNDANT_DGROUPS__: `false`
- __LDB_DGROUP_BACK_PROPAGATION__: `false`
- __LDB_METRICS__: `true`
