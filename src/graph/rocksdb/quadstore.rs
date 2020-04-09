use crate::graph::value::Value;
use crate::graph::refs::{Size, Ref, Namer, Content, Primitive};
use crate::graph::iterator::{Shape, Null};
use crate::graph::quad::{QuadStore, Quad, Direction, Stats, Delta, IgnoreOptions, Procedure};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use std::sync::{Arc, RwLock};
use std::ops::Bound;

use rocksdb::{DB, Options};


pub struct RocksDB {
    db: DB
}

impl RocksDB {
    pub fn open(path: String) -> RocksDB {
        RocksDB {
            db: DB::open_default(path).unwrap()
        }
    }
}


impl Namer for RocksDB {
    fn value_of(&self, v: &Value) -> Option<Ref> {
 
    }
 
    fn name_of(&self, key: &Ref) -> Option<Value> {
 
    }
}


impl QuadStore for RocksDB {
    fn quad(&self, r: &Ref) -> Option<Quad> {

    }

    fn quad_iterator(&self, d: &Direction, r: &Ref) -> Rc<RefCell<dyn Shape>> {

    }

    fn quad_iterator_size(&self, d: &Direction, r: &Ref) -> Result<Size, String> {

    }

    fn quad_direction(&self, r: &Ref, d: &Direction) -> Option<Ref> {

    }

    fn stats(&self, exact: bool) -> Result<Stats, String> {

    }
    
    fn apply_deltas(&mut self, deltas: Vec<Delta>, ignore_opts: &IgnoreOptions) -> Result<(), String> {

    }

    fn nodes_all_iterator(&self) -> Rc<RefCell<dyn Shape>> {

    }

    fn quads_all_iterator(&self) -> Rc<RefCell<dyn Shape>> {

    }

    fn close(&self) -> Option<String> {
        
    }
}
