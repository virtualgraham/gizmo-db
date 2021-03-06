use super::number::Number;
use super::value::Value;
use super::quad::{Quad, InternalQuad};



#[derive(PartialEq, Debug, Clone)]
pub struct Size {
    pub value: i64, // TODO: shouldnt this be u64?
    pub exact: bool
}

impl Size {
    pub fn new() -> Size {
        Size {
            value: 0,
            exact: true
        }
    }
}


pub trait Namer {
    fn value_of(&self, v: &Value) -> Option<Ref>;
    fn name_of(&self, key: &Ref) -> Option<Value>;
    
    #[allow(unused)]
    fn values_of(&self, values: &Vec<Ref>) -> Result<Vec<Value>, String> {
        Ok(values.iter().map(|v| self.name_of(v).unwrap()).collect())
    }

    #[allow(unused)]
    fn refs_of(&self, nodes: &Vec<Value>) -> Result<Vec<Ref>, String> {
        nodes.iter().map(|v| {
            match self.value_of(v) { Some(s) => Ok(s), None => Err("Not Found".to_string()) }
        }).collect()
    }
}


pub fn pre_fetched(v: Value) -> Ref {
    Ref {
        k: Some(v.calc_hash()),
        content: Content::Value(v),
    }
}

// #[derive(Debug, PartialEq, Clone)]
// pub struct Primitive {
//     subject: u64, 
//     predicate: u64,
//     object: u64,
//     label: u64,
// }

// impl Primitive {
//     pub fn get_direction(&self, dir: Direction) -> u64 {
//         match dir {
//             Direction::Subject => self.subject,
//             Direction::Predicate => self.predicate,
//             Direction::Object => self.object,
//             Direction::Label => self.label
//         }
//     }
// }

#[derive(Debug, PartialEq, Clone)]
pub enum Content {
    None,
    Value(Value),
    Quad(Quad),
    InternalQuad(InternalQuad)
}

#[derive(Debug, PartialEq, Clone)]
pub struct Ref {
    pub k: Option<u64>,
    pub content: Content
}

impl Ref {
    pub fn none() -> Ref {
        Ref {
            k: None,
            content: Content::None
        }
    }

    // a Ref with key Value::None or 0 is used to refer to an exsisting quad but the direction is unassigned
    // TODO: using 0 seems inconsistant, should replace all usage of 0 with None
    // this is often the case with the label direction
    // using this method helps to ensure we are checking and handling this scenerio properly
    pub fn key(&self) -> Option<u64> {
        if let Some(0) = self.k {
            return None
        }
        return self.k
    }

    pub fn new_i64_node(v: i64) -> Ref {
        let value = Value::Number(Number::from(v));
        Ref {
            k: Some(value.calc_hash()),
            content: Content::Value(value),
        }
    }

    pub fn unwrap_value(&self) -> &Value {
        match &self.content {
            Content::Value(v) => v,
            _ => panic!("Ref does not contain a value")
        }
    }

    pub fn unwrap_quad(&self) -> &Quad {
        match &self.content {
            Content::Quad(q) => q,
            _ => panic!("Ref does not contain a value")
        }
    }

    pub fn has_value(&self) -> bool {
        if let Content::Value(_) = self.content {
            return true
        }
        false
    }

    pub fn has_quad(&self) -> bool {
        if let Content::Quad(_) = self.content {
            return true
        }
        false
    }
}


// impl PartialEq<Value> for Content {
//     fn eq(&self, other: &Value) -> bool {
//         match self {
//             Content::Value(v) => other.eq(v),
//             _ => false
//         }
//     }
// }


// impl PartialEq<Quad> for Content {
//     fn eq(&self, other: &Quad) -> bool {
//         match self {
//             Content::Quad(q) => other.eq(q),
//             _ => false
//         }
//     }
// }


impl PartialEq<Option<Value>> for Content {
    fn eq(&self, other: &Option<Value>) -> bool {
        match self {
            Content::None => other.is_none(),
            Content::Value(v) => other.is_some() && other.as_ref().unwrap().eq(v),
            _ => false
        }
    }
}


impl PartialEq<Option<Quad>> for Content {
    fn eq(&self, other: &Option<Quad>) -> bool {
        match self {
            Content::None => other.is_none(),
            Content::Quad(q) => other.is_some() && other.as_ref().unwrap().eq(q),
            _ => false
        }
    }
}

