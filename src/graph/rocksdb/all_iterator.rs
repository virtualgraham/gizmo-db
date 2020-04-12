use crate::graph::refs::{Size, Ref};
use crate::graph::iterator::{Base, Scanner, Index, Shape, Costs, ShapeType};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use rocksdb::IteratorMode;

use super::quadstore::{InternalRocksDB, Primitive, primitive_key, PRIMITIVE_KEY_PREFIX};

use std::sync::Arc;


pub struct RocksDbAllIterator {
    db: Arc<InternalRocksDB>,
    nodes: bool
}

impl RocksDbAllIterator {
    pub fn new(db: Arc<InternalRocksDB>, nodes: bool) -> Rc<RefCell<RocksDbAllIterator>> {
        Rc::new(RefCell::new(RocksDbAllIterator {
            db,
            nodes
        }))
    }
}


impl Shape for RocksDbAllIterator {

    fn iterate(&self) -> Rc<RefCell<dyn Scanner>> {
        RocksDbAllIteratorNext::new(self.db.clone(), self.nodes)
    }

    fn lookup(&self) -> Rc<RefCell<dyn Index>> {
        RocksDbAllIteratorContains::new(self.db.clone(), self.nodes)
    }

    fn stats(&mut self) -> Result<Costs, String> {
        let count = self.db.get_count()?;

        Ok(Costs {
            contains_cost: 1,
            next_cost: 1,
            size: Size {
                value: count.total() as i64,
                exact: true
            }
        })
    }

    fn optimize(&mut self) -> Option<Rc<RefCell<dyn Shape>>> {
        None
    }

    fn sub_iterators(&self) -> Option<Vec<Rc<RefCell<dyn Shape>>>> {
        None
    }

    fn shape_type(&mut self) -> ShapeType {
        ShapeType::StoreIterator
    }

}



pub struct RocksDbAllIteratorNext {
    db: Arc<InternalRocksDB>,
    nodes: bool,
    done: bool,
    cur: Option<Ref>
}


impl RocksDbAllIteratorNext {
    pub fn new(db: Arc<InternalRocksDB>, nodes: bool) -> Rc<RefCell<RocksDbAllIteratorNext>> {
        Rc::new(RefCell::new(RocksDbAllIteratorNext {
            db,
            nodes,
            done: false,
            cur: None
        }))
    }
}


impl Base for RocksDbAllIteratorNext {
    fn tag_results(&self, _tags: &mut HashMap<String, Ref>) {}

    fn result(&self) -> Option<Ref> {
        return self.cur.clone()
    }

    fn next_path(&mut self) -> bool {
        false
    }

    fn err(&self) -> Option<String> {
        None
    }

    fn close(&mut self) -> Result<(), String> {
        self.done = true;
        Ok(())
    }
}


impl Scanner for RocksDbAllIteratorNext {
    fn next(&mut self) -> bool {
        
        if self.done {
            return false
        }

        // TODO: node and quad primitives should have a different prefix, this would require changing Ref to know if the key is for a value or quad 

        let lam = |(_, v):(Box<[u8]>, Box<[u8]>)| {
            match Primitive::decode(&v) {
                Ok(p) => {
                    let is_node = p.is_node();

                    if self.nodes && is_node {
                        return Some(p)
                    } else if !self.nodes && !is_node {
                        return Some(p)
                    } 
    
                    return None
                },
                Err(_) => {
                    // TODO: result() should return Result<Option<>>
                    return None
                }
            }
        };

        self.cur = if !self.done && self.cur.is_none() {

            self.db.db.iterator(
                IteratorMode::Start
            ).take_while(|(k,_)| {
                !k.is_empty() && k[0] == PRIMITIVE_KEY_PREFIX
            }).filter_map(
                lam
            ).map(|p| {
                p.to_ref(self.nodes).unwrap()
            }).next()

        } else {

            self.db.db.iterator(
                IteratorMode::From(&primitive_key(self.cur.as_ref().unwrap().k.unwrap() + 1), rocksdb::Direction::Forward)
            ).take_while(|(k,_)| {
                !k.is_empty() && k[0] == PRIMITIVE_KEY_PREFIX
            }).filter_map(
                lam
            ).map(|p| {
                p.to_ref(self.nodes).unwrap()
            }).next()

        };

        if !self.cur.is_some() {
            self.done = true;
            return false
        }

        return true
    }
}



pub struct RocksDbAllIteratorContains {
    db: Arc<InternalRocksDB>,
    nodes: bool,
    cur: Option<Ref>,
    done: bool
}


impl RocksDbAllIteratorContains {
    pub fn new(db: Arc<InternalRocksDB>, nodes: bool) -> Rc<RefCell<RocksDbAllIteratorContains>> {
        Rc::new(RefCell::new(RocksDbAllIteratorContains {
            db,
            nodes,
            cur: None,
            done: false
        }))
    }
}


impl Base for RocksDbAllIteratorContains {
    fn tag_results(&self, _tags: &mut HashMap<String, Ref>) {}

    fn result(&self) -> Option<Ref> {
        return self.cur.clone()
    }

    fn next_path(&mut self) -> bool {
        false
    }

    fn err(&self) -> Option<String> {
        None
    }

    fn close(&mut self) -> Result<(), String> {
        self.done = true;
        Ok(())
    }
}


impl Index for RocksDbAllIteratorContains {
    fn contains(&mut self, v:&Ref) -> bool {
        if self.done {
            return false
        }

        let id = v.key();
        
        match id {
            Some(i) => {
                match self.db.get_primitive(i) {
                    Ok(prim) => {
                        if let Some(p) = prim {
                           self.cur = p.to_ref(self.nodes);
                           return true
                        }
                        self.cur = None;
                        return false  
                    },
                    Err(_) => {
                        // TODO: change impl to return result
                        return false
                    }
                }
            },
            None => return false
        }
    }
}