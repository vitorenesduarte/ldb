# ldb 

[![Build Status](https://travis-ci.org/vitorenesduarte/ldb.svg?branch=master)](https://travis-ci.org/vitorenesduarte/ldb/)

LDB supports different CRDT replication models:
- state-based
- delta-state-based
- scuttebutt-based
- op-based

### More info
- [Efficient Synchronization of State-based CRDTs](https://vitorenes.org/publication/enes-efficient-synchronization/) Vitor Enes, Paulo Sérgio Almeida, Carlos Baquero, João Leitão. In _ICDE_, 2019
- [Delta State Replicated Data Types](https://arxiv.org/pdf/1603.01529.pdf), Paulo Sérgio Almeida, Ali Shoker, Carlos Baquero. In _J. Parallel Distrib. Comput._, 2018
- [Efficient Reconciliation and Flow Control for Anti-Entropy Protocols](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf), Robbert van Renesse, Dan Dumitriu, Valient Gough, Chris Thomas. In _LADIS_, 2008.
- [Conflict-free Replicated Data Types](https://hal.inria.fr/inria-00609399v1/document), Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski. In _SSS_, 2011.
- [Join Decompositions for Efficient Synchronization of CRDTs after a Network Partition](https://vitorenes.org/publication/enes-join-decompositions/enes-join-decompositions.pdf), Vitor Enes, Carlos Baquero, Paulo Sérgio Almeida, Ali Shoker. In _PMLDC@ECOOP_, 2016.
- [Efficient Synchronization of State-based CRDTs](https://vitorenes.org/page/other/msc-thesis.pdf), Vitor Enes. Msc Thesis, 2017.

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
- __LDB_OP_II__: when set to `true`, the backend leverages implicit info in protocol messages

||||||
|---------------------------------|---------------|------------------|------------------|------------------|
| __NODE_NUMBER__                 | `0..`         | `0..`            | `0..`            | `0..`            |
| __LDB_MODE__                    | `state_based` | `delta_based`    | `scuttlebutt`    | `op_based`       |
| __LDB_STATE_SYNC_INTERVAL__     | `0..`         | `0..`            | `0..`            | `0..`            |
| __LDB_REDUNDANT_DGROUPS__       | __NA__        | `true` / `false` | __NA__           | __NA__           |
| __LDB_DGROUP_BACK_PROPAGATION__ | __NA__        | `true` / `false` | __NA__           | __NA__           |
| __LDB_SCUTTLEBUTT_GC__          | __NA__        | __NA__           | `true` / `false` | __NA__           |
| __LDB_OP_II__                   | __NA__        | __NA__           | __NA__           | `true` / `false` |

#### Defaults
- __LDB_MODE__: `state_based`
- __LDB_STATE_SYNC_INTERVAL__: 1000
- __LDB_REDUNDANT_DGROUPS__: `false`
- __LDB_DGROUP_BACK_PROPAGATION__: `false`
- __LDB_SCUTTLEBUTT_GC__: `false`
- __LDB_OP_II__: `false`

#### Experiments
Please check [https://github.com/vitorenesduarte/exp](https://github.com/vitorenesduarte/exp).
