# ldb 

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- scuttebutt-based
- op-based


### More info
- [Efficient Synchronization of State-based CRDTs](https://arxiv.org/pdf/1803.02750.pdf)
- [Delta State Replicated Data Types](https://arxiv.org/pdf/1603.01529.pdf)
- [Efficient Reconciliation and Flow Control for Anti-Entropy Protocols](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf))
- [Conflict-free Replicated Data Types](https://pages.lip6.fr/Marc.Shapiro/papers/RR-7687.pdf)
- [Join Decompositions for Efficient Synchronization of CRDTs after a Network Partition](https://vitorenes.org/publication/enes-join-decompositions/enes-join-decompositions.pdf)
- [Efficient Synchronization of State-based CRDTs (MSc Thesis)](https://vitorenes.org/page/other/msc-thesis.pdf)

### Configuration
- __LDB_MODE__:
  - `state_based`
  - `delta_based`
  - `scuttlebutt`
  - `op_based`
- __LDB_STATE_SYNC_INTERVAL__: state is propagated every `X` milliseconds
- __LDB_REDUNDANT_DGROUPS__: when set to `true`,
removes redundant state that may be present in the received
delta-groups, using join-decompositions
- __LDB_DGROUP_BACK_PROPAGATION__: when set to `true`,
avoids back-propagation of delta-groups
- __LDB_SCUTTLEBUTT_GC__: when set to `true`, performs garbage collection of buffer entries in Scuttlebutt.

|||||
|---------------------------------|---------------|------------------|------------------|------------|
| __NODE_NUMBER__                 | `0..`         | `0..`            | `0..`            | `0..`      |
| __LDB_MODE__                    | `state_based` | `delta_based`    | `scuttlebutt`    | `op_based` |
| __LDB_STATE_SYNC_INTERVAL__     | `0..`         | `0..`            | `0..`            | `0..`      |
| __LDB_REDUNDANT_DGROUPS__       | __NA__        | `true` / `false` | __NA__           | __NA__     |
| __LDB_DGROUP_BACK_PROPAGATION__ | __NA__        | `true` / `false` | __NA__           | __NA__     |
| __LDB_SCUTTLEBUTT_GC__          | __NA__        | __NA__           | `true` / `false` | __NA__     |

#### Defaults
- __LDB_MODE__: `state_based`
- __LDB_STATE_SYNC_INTERVAL__: 1000
- __LDB_REDUNDANT_DGROUPS__: `false`
- __LDB_DGROUP_BACK_PROPAGATION__: `false`
- __LDB_SCUTTLEBUTT_GC__: `false`
