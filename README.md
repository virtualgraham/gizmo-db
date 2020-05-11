## Gizmo Graph DB

Gizmo is an embeddable quadstore graph database with a powerful query engine. It is written in Rust and supports both in-memory and [RocksDB](https://rocksdb.org/) backed stores. The query engine is ported from [Cayley Graph Database](https://github.com/cayleygraph/cayley) The query language [Gizmo](https://cayley.gitbook.io/cayley/gizmoapi), a dialect of [Gremlin](https://tinkerpop.apache.org/gremlin.html), is mostly identical to Cayley except for a few changes to adapt to Rust language requirements. 
