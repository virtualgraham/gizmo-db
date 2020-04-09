mod query;
mod graph;
#[cfg(feature = "standalone")]
use query::gizmo;
use graph::quad::Quad;
use graph::value::Value;




fn main() {
    let v_none = Value::None;
    let e = v_none.encode();
    let s = std::str::from_utf8(&e).unwrap();
    println!("{}", s)
}


// #[cfg(feature = "standalone")]
// fn main() {
 
//     let simple_graph = gizmo::new_memory_graph();

//     simple_graph.write(vec![
//         Quad::new("<alice>", "<follows>", "<bob>", ()),
//         Quad::new("<bob>", "<follows>", "<fred>", ()),
//         Quad::new("<bob>", "<status>", "cool_person", ()),

//         Quad::new("<dani>", "<follows>", "<bob>", ()),
//         Quad::new("<charlie>", "<follows>", "<bob>", ()),
//         Quad::new("<charlie>", "<follows>", "<dani>", ()),

//         Quad::new("<dani>", "<follows>", "<greg>", ()),
//         Quad::new("<dani>", "<status>", "cool_person", ()),
//         Quad::new("<emily>", "<follows>", "<fred>", ()),

//         Quad::new("<fred>", "<follows>", "<greg>", ()),
//         Quad::new("<greg>", "<status>", "cool_person", ()),
//         Quad::new("<predicates>", "<are>", "<follows>", ()),

//         Quad::new("<predicates>", "<are>", "<status>", ()),
//         Quad::new("<emily>", "<status>", "smart_person", "<smart_graph>"),
//         Quad::new("<greg>", "<status>", "smart_person", "<smart_graph>")
//     ]);

  
//     let read_result:Vec<Quad> = simple_graph.read(None, None, None, None).collect();
    
//     println!("{:?}", read_result);

//     // let g = simple_graph.g();

    
//     // let mut r:Vec<String> = g.v(vec!["<bob>", "<charle>"])
//     //     .out("<follows>", None)
//     //     .save_opt("<status>", "somecool")
//     //     .iter().map(|x| x["somecool"].to_string()).collect();

//     // let mut f:Vec<String> = vec![
//     //     "cool_person".into(),
//     //     "cool_person".into()
//     // ];

//     // r.sort();
//     // f.sort();

//     // assert_eq!(r, f);

//     /////////////////////////
//     // show a simple save
//     /////////////////////////

//     // let mut r:Vec<HashMap<String, Value>> = g.v(None)
//     //     .save("<status>", "somecool")
//     //     .iter().collect();

//     // println!("r: {:?}", r);


//     // let r:Vec<String> = g.v(vec!["<bob>", "<charlie>"])
//     //     .out("<follows>", None)
//     //     .save_opt("<status>", "somecool")
//     //     .iter_tags().filter_map(|x| x.get("somecool").map(|v| v.to_string())).collect();

//     // println!("r: {:?}", r);


//     // let mut r:Vec<HashMap<String, Value>> = g.v(vec!["<bob>", "<charlie>"]).out("<follows>", None).iter().collect();

//     // println!("r: {:?}", r);

// }


// #[cfg(not(feature = "standalone"))]
// fn main() {
//     println!("Not Configured For Standalone");
// }