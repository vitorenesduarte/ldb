# ldb 

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- pure-op-based

### Configuration
- __LDB_MODE__:
  - `state_based`
  - `delta_based`
  - `pure_op_based`
- __LDB_DRIVEN_MODE__: see [this](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf)
for details on `state_driven` and `digest_driven` algorithms
  - `none`
  - `state_driven`
  - `digest_driven`
- __LDB_STATE_SYNC_INTERVAL__: in `state_based` and `delta_based`
modes, state is propagated every `X` milliseconds
- __LDB_EVICTION_ROUND_NUMBER__: evict a peer from the ack map if no
ack is received from this peer after `X` rounds of state
synchronization (where messages where sent to this peer).
If this value is `-1`, peer eviction is not performed
- __LDB_REDUNDANT_DGROUPS__: when set to `true`,
removes redundant state that may be present in the received
delta-groups, using [join-decompositions](http://haslab.uminho.pt/cbm/files/pmldc-2016-join-decomposition.pdf)
- __LDB_DGROUP_BACK_PROPAGATION__: when set to `true`,
avoids back-propagation of delta-groups
- __LDB_METRICS__: metrics are recorded if `true`

|||||
|---------------------------------|-------------------------------------------|-------------------------------------------|-----------------|
| __LDB_MODE__                    | `state_based`                             | `delta_based`                             | `pure_op_based` |
| __LDB_DRIVEN_MODE__             | `none` / `state_driven` / `digest_driven` | `none` / `state_driven` / `digest_driven` | __NA__          |
| __LDB_STATE_SYNC_INTERVAL__     | `0..`                                     | `0..`                                     | __NA__          |
| __LDB_EVICTION_ROUND_NUMBER__   | __NA__                                    | `-1..`                                    | __NA__          |
| __LDB_REDUNDANT_DGROUPS__       | __NA__                                    | `true` / `false`                          | __NA__          |
| __LDB_DGROUP_BACK_PROPAGATION__ | __NA__                                    | `true` / `false`                          | __NA__          |

#### Defaults
- __LDB_MODE__: `state_based`
- __LDB_DRIVEN_MODE__: `none`
- __LDB_STATE_SYNC_INTERVAL__: 1000
- __LDB_EVICTION_ROUND_NUMBER__: -1
- __LDB_REDUNDANT_DGROUPS__: `false`
- __LDB_DGROUP_BACK_PROPAGATION__: `false`
- __LDB_METRICS__: `false`
