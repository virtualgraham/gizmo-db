use crate::graph::refs::{Size, Ref, Content};
use crate::graph::iterator::{Base, Scanner, Index, Shape, Costs, ShapeType};
use crate::graph::quad::{Direction};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};



pub struct QuadIds {
    quad_ids: Rc<BTreeSet<u64>>,
    d: Direction
}

impl QuadIds {
    pub fn new(quad_ids: Rc<BTreeSet<u64>>, d: Direction) -> Rc<RefCell<QuadIds>> {
        Rc::new(RefCell::new(QuadIds {
            quad_ids,
            d
        }))
    }
}

impl Shape for QuadIds {

    fn iterate(&self) -> Rc<RefCell<dyn Scanner>> {
        QuadIdsNext::new(self.quad_ids.clone(), self.d.clone())
    }

    fn lookup(&self) -> Rc<RefCell<dyn Index>> {
        QuadIdsContains::new(self.quad_ids.clone(), self.d.clone())
    }

    fn stats(&mut self) -> Result<Costs, String> {
        Ok(Costs {
            contains_cost: ((self.quad_ids.len() as f64).ln() as i64) + 1,
            next_cost: 1,
            size: Size {
                value: self.quad_ids.len() as i64,
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
        ShapeType::QuadIds
    }

}


pub struct QuadIdsNext {
    quad_ids: Rc<BTreeSet<u64>>,
    d: Direction,
    cur: Option<u64>,
    done: bool
}

impl QuadIdsNext {
    pub fn new(quad_ids: Rc<BTreeSet<u64>>, d: Direction) -> Rc<RefCell<QuadIdsNext>> {
        
        Rc::new(RefCell::new(QuadIdsNext {
            quad_ids,
            d,
            cur: None,
            done: false
        }))
        
    }
}

impl Base for QuadIdsNext {
    fn tag_results(&self, _tags: &mut HashMap<String, Ref>) {}

    fn result(&self) -> Option<Ref> {
        match self.cur {
            Some(quad_id) => Some(Ref {
                k: Some(quad_id),
                content: Content::None
            }),
            None => None
        }
    }

    fn next_path(&mut self) -> bool {
        false
    }

    fn err(&self) -> Option<String> {
        None
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Scanner for QuadIdsNext {
    fn next(&mut self) -> bool {

        if self.done {
            return false
        }
        
        // TODO: This is ridiculous, there has to be a way to just use a single iterator.

        self.cur = if !self.done && self.cur.is_none() {
            self.quad_ids.iter().map(|quad_id| *quad_id).next()
        } else {
            self.quad_ids.range((Excluded(self.cur.unwrap()), Unbounded)).map(|quad_id| *quad_id).next()
        };

        if !self.cur.is_some() {
            self.done = true;
            return false
        }

        return true
    }
}



pub struct QuadIdsContains {
    quad_ids: Rc<BTreeSet<u64>>,
    d: Direction,
    cur: Option<u64>
}

impl QuadIdsContains {
    pub fn new(quad_ids: Rc<BTreeSet<u64>>, d: Direction) -> Rc<RefCell<QuadIdsContains>> {
        Rc::new(RefCell::new(QuadIdsContains {
            quad_ids,
            d,
            cur: None
        }))
    }
}

impl Base for QuadIdsContains {
    fn tag_results(&self, _tags: &mut HashMap<String, Ref>) {}

    fn result(&self) -> Option<Ref> {
        match self.cur {
            Some(c) => Some(Ref {
                k: Some(c),
                content: Content::None
            }),
            None => None
        }
    }

    fn next_path(&mut self) -> bool {
        false
    }

    fn err(&self) -> Option<String> {
        None
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Index for QuadIdsContains {
    fn contains(&mut self, v:&Ref) -> bool {
        match v.key() {
            Some(i) => {
                let c = self.quad_ids.contains(&i);
                if c {
                    self.cur = Some(i);
                    return true
                } else {
                    self.cur = None;
                    return false
                }
            },
            None => {
                self.cur = None;
                return false
            }
        }
    }
}

