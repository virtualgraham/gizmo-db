## Gizmo Graph DB

Gizmo is an embeddable, quadstore, graph database with a powerful query engine. It is written in Rust and supports both in-memory and [RocksDB](https://rocksdb.org/) backed stores. The query engine is ported from [Cayley Graph Database](https://github.com/cayleygraph/cayley) The query language [Gizmo](https://cayley.gitbook.io/cayley/gizmoapi), a dialect of [Gremlin](https://tinkerpop.apache.org/gremlin.html), is identical to Cayley's with only minor changes to adapt to Rust language requirements. 


```Rust
    use gizmo_db::query::gizmo;
    use gizmo_db::graph::quad::Quad;

    let simple_graph = gizmo::new_rocksdb_graph("gizmo_tests.db");

    simple_graph.write(vec![
        Quad::new("<alice>", "<follows>", "<bob>", ()),
        Quad::new("<bob>", "<follows>", "<fred>", ()),
        Quad::new("<bob>", "<status>", "cool_person", ()),

        Quad::new("<dani>", "<follows>", "<bob>", ()),
        Quad::new("<charlie>", "<follows>", "<bob>", ()),
        Quad::new("<charlie>", "<follows>", "<dani>", ()),

        Quad::new("<dani>", "<follows>", "<greg>", ()),
        Quad::new("<dani>", "<status>", "cool_person", ()),
        Quad::new("<emily>", "<follows>", "<fred>", ()),

        Quad::new("<fred>", "<follows>", "<greg>", ()),
        Quad::new("<greg>", "<status>", "cool_person", ()),
        Quad::new("<predicates>", "<are>", "<follows>", ()),

        Quad::new("<predicates>", "<are>", "<status>", ()),
        Quad::new("<emily>", "<status>", "smart_person", "<smart_graph>"),
        Quad::new("<greg>", "<status>", "smart_person", "<smart_graph>")
    ]);


    /////////////////////////
    // use .in() with .filter()
    /////////////////////////

    let mut r:Vec<String> = g
        .v("<bob>")
        .r#in("<follows>", None)
        .filter(vec![gizmo::gt("<c>"), gizmo::lt("<d>")])
        .iter_values().map(|v| v.to_string()).collect();


    let mut f:Vec<String> = vec![
        "<charlie>".into()
    ];

    r.sort();
    f.sort();

    assert_eq!(r, f);


    /////////////////////////
    // show reverse morphism
    /////////////////////////

    let grandfollows = g.m().out("<follows>", None).out("<follows>", None);

    let mut r:Vec<String> = g
        .v("<fred>")
        .follow_r(&grandfollows)
        .iter_values().map(|v| v.to_string()).collect();


    let mut f:Vec<String> = vec![
        "<alice>".into(),
        "<charlie>".into(),
        "<dani>".into()
    ];

    r.sort();
    f.sort();

    assert_eq!(r, f);

```