use crate::graph::value::Value;
use crate::graph::refs::{Size, Ref, Namer, Content};
use crate::graph::iterator::{Shape, Null};
use crate::graph::quad::{QuadStore, InternalQuad, Quad, Direction, Stats, Delta, IgnoreOptions, Procedure};
use crate::graph::iterator::quad_ids::QuadIds;

use std::rc::Rc;
use std::cell::RefCell;

use rocksdb::{DB, IteratorMode};

use std::hash::Hash;

use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt, ByteOrder};

use std::collections::BTreeSet;

use std::sync::Arc;

use super::all_iterator::RocksDbAllIterator;

pub struct InternalRocksDB {
    pub db: DB
}

impl InternalRocksDB {
    fn open(path: &str) -> Result<InternalRocksDB, String> {
        Ok(InternalRocksDB {
            db: match DB::open_default(path) {
                Ok(db) => db,
                Err(_) => return Err("Unable to open database file".to_string())
            }
        })
    }


    // Primitives

    pub fn get_count(&self) -> Result<PrimitiveCount, String> {
        match self.db.get(&[PRIMITIVE_COUNT_KEY]) {
            Ok(Some(bytes)) => {
                Ok(PrimitiveCount::decode(&bytes))
            }
            Ok(None) => {
                Ok(PrimitiveCount::zero())
            }
            _ => return Err("read/write error encountered".to_string())
        }
    }

    pub fn get_primitive(&self, id: u64) -> Result<Option<Primitive>, String> {
        let key = primitive_key(id);

        match self.db.get(key) {
            Ok(Some(pr)) => Ok(Some(Primitive::decode(&pr)?)),
            Ok(None) => Ok(None),
            Err(_) => Err("read/write error encountered".to_string())
        }
    }

    // Only call this method after you have checked that the primitive does not yet exist
    fn add_primitive(&self, p: &mut Primitive) -> Result<u64, String> {
        let mut count = self.get_count()?;
        match p.content {
            PrimitiveContent::Value(_) => {
                count.increment_values(1);
            },
            PrimitiveContent::InternalQuad(_) => {
                count.increment_quads(1);
            }
        };

        p.id = count.total();
        p.refs = 1;

        self.db.put(primitive_key(p.id), p.encode())?;

        self.add_id_hash(p.calc_hash(), p.id)?;

        self.db.put([PRIMITIVE_COUNT_KEY], count.encode())?;

        Ok(p.id)
    }
    
    fn remove_primitive(&self, p: &Primitive) -> Result<(), String> {
        self.db.delete(primitive_key(p.id))?;
        self.remove_id_hash(p.calc_hash())?;

        let mut count = self.get_count()?;
        match p.content {
            PrimitiveContent::Value(_) => {
                count.increment_values(-1);
            },
            PrimitiveContent::InternalQuad(_) => {
                count.increment_quads(-1);
            }
        };
        self.db.put([PRIMITIVE_COUNT_KEY], count.encode())?;

        Ok(())
    }

    // Quad Direction Index

    fn get_quad_direction(&self, direction: &Direction, value_id: &u64) -> BTreeSet<u64> {
        let lower_bound = quad_direction_key(value_id.clone(), direction, 0);
        self.db.iterator(IteratorMode::From(&lower_bound, rocksdb::Direction::Forward))
        .take_while(|(k,_)| {
            !k.is_empty() && k[0] == QUAD_DIRECTION_KEY_PREFIX
        })
        .map(|(k,_)| decode_quad_direction_key(&k))
        .take_while(|(v, d, _)| {
            v == value_id && d == direction
        })
        .map(|(_, _, quad_id)| quad_id)
        .collect()
    }

    fn add_quad_direction(&self, value_id: u64, direction: &Direction, quad_id: u64) -> Result<(), String> {
        // TODO: keep a count of value_id: u64, direction: &Direction. but the only funtion that uses it is never used
        self.db.put(quad_direction_key(value_id, direction, quad_id), [])?;
        Ok(())
    }

    fn remove_quad_direction(&self, value_id: u64, direction: &Direction, quad_id: u64) -> Result<(), String> {
        // TODO: keep a count of value_id: u64, direction: &Direction. but the only funtion that uses it is never used
        self.db.delete(quad_direction_key(value_id, direction, quad_id))?;
        Ok(())
    }

    // Hash Index

    fn get_id_hash(&self, hash: u64) -> Result<Option<u64>, String> {
        let key = id_hash_key(hash);

        match self.db.get(key) {
            Ok(Some(bytes)) => Ok(Some(BigEndian::read_u64(&bytes))),
            Ok(None) => Ok(None),
            Err(_) => Err("read/write error encountered".to_string())
        }
    }

    fn add_id_hash(&self, hash: u64, id: u64) -> Result<(), String> {
        self.db.put(id_hash_key(hash), id.to_be_bytes())?;
        Ok(())
    }

    fn remove_id_hash(&self, hash: u64) -> Result<(), String> {
        self.db.delete(id_hash_key(hash))?;
        Ok(())
    }

    ///////////////////////


    fn resolve_val(&self, v: &Value, add: bool) -> Result<Option<u64>, String> {
        if let Value::None = v {
            return Ok(None)
        }

        let hash = v.calc_hash();
        
        let prim = if let Some(id) = self.get_id_hash(hash)?{
            self.get_primitive(id)?
        } else {
            None
        };
        
        if prim.is_some() || !add {
            // if the value exsists and we are adding it, increment refs
            let res = prim.as_ref().map(|p| p.id);
            
            if prim.is_some() && add {
                let mut p = prim.unwrap();
                p.refs += 1;
                self.db.put(primitive_key(p.id), p.encode())?; // update p.refs
            }
            
            return Ok(res)
        }

        let id = self.add_primitive(&mut Primitive::new_value(v.clone()))?;

        Ok(Some(id))
    }

    fn resolve_quad(&self, q: &Quad, add: bool) -> Result<Option<InternalQuad>, String> {
        let mut p = InternalQuad{s: 0, p: 0, o: 0, l: 0};

        // find all value ids for each direction of quad
        for dir in Direction::iterator() {
            let v = q.get(dir);
            if let Value::None = v {
                continue
            }
            let vid = self.resolve_val(v, add)?;
            if  let Some(i) = vid {
                p.set_dir(dir, i);
            } else {
                // if any value is not found or undefined return zero value internal quad
                return Ok(None)
            }
        }

        return Ok(Some(p))
    }

    fn find_quad(&self, q: &Quad) -> Result<Option<u64>, String> {
        let quad = self.resolve_quad(q, false)?;
        if let Some(q) = quad {
            return self.get_id_hash(q.calc_hash())
        }
        Ok(None)
    }


    fn delete_quad_nodes(&self, q: &InternalQuad) -> Result<(), String> {
        for dir in Direction::iterator() {
            let id = q.dir(dir);
            if id == 0 {
                continue
            }

            if let Some(mut p) = self.get_primitive(id)? { // value

                if p.refs == 0 {
                    return Err("remove of delete node".to_string())
                } 

                p.refs -= 1;
                
                if p.refs == 0 {

                    self.remove_primitive(&p)?;

                } else {

                    if let Err(_) = self.db.put(primitive_key(id), p.encode()) { // value
                        return Err("read/write error".to_string())
                    }

                }
            }
        }

        Ok(())
    }

    fn resolve_quad_default(&self, q: &Quad, add: bool) -> Result<InternalQuad, String> {
        match self.resolve_quad(q, add)? {
            Some(q) => Ok(q),
            None => Ok(InternalQuad{s: 0, p: 0, o: 0, l: 0})
        }
    }


    fn delete(&self, id: u64) -> Result<bool, String> {
        let mut quad:Option<InternalQuad> = None;
 
        if let Some(p) = self.get_primitive(id)? {
            if let PrimitiveContent::InternalQuad(q) = &p.content {
                quad = Some(q.clone());
            }

            self.remove_primitive(&p)?;
        } else {
            return Ok(false)
        }
        
        if let Some(q) = quad {
            for d in Direction::iterator() {
                self.remove_quad_direction(q.dir(d), d, id)?;
            }

            self.delete_quad_nodes(&q)?;
        }

        return Ok(true)
    }

    fn add_quad(&self, q: Quad) -> Result<u64, String> {
        // get value_ids for each direction
        let p = self.resolve_quad_default(&q, false)?;

        // get quad id
        let hash = p.calc_hash();

        let prim = if let Some(id) = self.get_id_hash(hash)?{
            self.get_primitive(id)?
        } else {
            None
        };

        // if prim already exsits
        if let Some(p) = prim {
            return Ok(p.id)
        }

        // get value_ids for each direction, this time inserting the values as neccecery
        let p = self.resolve_quad_default(&q, true)?;

        // add value primitive
        let mut pr = Primitive::new_quad(p.clone());
        let id = self.add_primitive(&mut pr)?;

        // add to index
        for d in Direction::iterator() {
            self.add_quad_direction(p.dir(d), d, id)?;
        }

        return Ok(id);
    }

    fn lookup_val(&self, id: u64) -> Result<Option<Value>, String> {
        match self.get_primitive(id)? {
            Some(p) => {
                match p.content {
                    PrimitiveContent::Value(v) => Ok(Some(v)),
                    _ => Ok(None)
                }
            },
            None => Ok(None)
        }
    }

    fn internal_quad(&self, r: &Ref) -> Result<Option<InternalQuad>, String> {
        let key = if let Some(k) = r.key() { 
            self.get_primitive(k)?
        } else { 
            None 
        };

        match key {
            Some(p) => {
                match p.content {
                    PrimitiveContent::InternalQuad(q) => Ok(Some(q)),
                    _ => Ok(None)
                }
            },
            None => Ok(None)
        }
    }

    fn lookup_quad_dirs(&self, p: InternalQuad) -> Result<Quad, String> {
        let mut q = Quad::new_undefined_vals();
        for dir in Direction::iterator() {
            let vid = p.dir(dir);
            if vid == 0 {
                continue
            }
            let val = self.lookup_val(vid)?;
            if let Some(v) = val {
                q.set_val(dir, v);
            }
        }
        return Ok(q)
    }



}



pub struct RocksDB {
    store: Arc<InternalRocksDB>
}

impl RocksDB {
    pub fn open(path: &str) -> Result<RocksDB, String> {
        Ok(RocksDB {
            store: Arc::new(InternalRocksDB::open(path)?)
        })
    }
}

impl Namer for RocksDB {
    fn value_of(&self, v: &Value) -> Option<Ref> {
        if let Value::None = v {
            return None
        }

        let hash = v.calc_hash();

        if let Ok(Some(id)) = self.store.get_id_hash(hash) { // TODO: this method should return Result<Option>
            Some(Ref {
                k: Some(id),
                content: Content::Value(v.clone())
            })
        } else {
            None
        }
    }
 
    fn name_of(&self, key: &Ref) -> Option<Value> {
        if let Content::Value(v) = &key.content {
            return Some(v.clone())
        }

        if let Some(i) = key.key() {
            if let Ok(v) = self.store.lookup_val(i) {
                return v
            } else {    
                // TODO: return Err
                return None
            }
        } else {
            return None
        }
    }
}


impl QuadStore for RocksDB {
    fn quad(&self, r: &Ref) -> Option<Quad> {

        let internal_quad:Option<InternalQuad> = match &r.content {
            Content::Quad(q) => {
                return Some(q.clone())
            },
            Content::InternalQuad(iq) => {
                Some(iq.clone())
            }
            _ => {
                match self.store.internal_quad(r) {
                    Ok(iq) => {
                        iq
                    },
                    Err(_) => {
                        // TODO: return Err
                        return None
                    } 
                }
            }
        };

        match internal_quad {
            Some(q) => {
                if let Ok(dirs) = self.store.lookup_quad_dirs(q) {
                    return Some(dirs)
                } else {
                    // TODO: return Err
                    return None
                }
            }
            None => None
        }
    }

    fn quad_iterator(&self, d: &Direction, r: &Ref) -> Rc<RefCell<dyn Shape>> {
        if let Some(i) = r.key() {

            let quad_ids = self.store.get_quad_direction(d, &i);

            if !quad_ids.is_empty() {
                return QuadIds::new(Rc::new(quad_ids), d.clone())
            }
        } 
            
        Null::new()
    }

    fn quad_iterator_size(&self, d: &Direction, r: &Ref) -> Result<Size, String> {
        if let Some(i) = r.key() {

            let quad_ids = self.store.get_quad_direction(d, &i);

            if !quad_ids.is_empty() {
                return Ok(Size{value: quad_ids.len() as i64, exact: true})
            }
        } 
            
        return Ok(Size{value: 0, exact: true})
    }

    fn quad_direction(&self, r: &Ref, d: &Direction) -> Option<Ref> {
        let quad = match self.store.internal_quad(r) {
            Ok(q) => q,
            Err(_) => {
                return None
                // TODO: return Result<Option>>
            }
        };

        match quad {
            Some(q) => {
                let id = q.dir(d);
                if id == 0 {
                    // The quad exsists, but the value is none
                    return Some(Ref::none())
                }
                return Some(Ref {
                    k: Some(id),
                    content: Content::None
                })
            }
            // the quad does not exsist
            None => None
        }
    }

    fn stats(&self, _exact: bool) -> Result<Stats, String> {
        let count = self.store.get_count()?;

        Ok(Stats {
            nodes: Size {
                value: count.values as i64,
                exact: true
            },
            quads: Size {
                value: count.quads as i64,
                exact: true
            }
        })
    }
    
    fn apply_deltas(&mut self, deltas: Vec<Delta>, ignore_opts: &IgnoreOptions) -> Result<(), String> {
        if !ignore_opts.ignore_dup || !ignore_opts.ignore_missing {
            for d in &deltas {
                match d.action {
                    Procedure::Add => {
                        if !ignore_opts.ignore_dup {
                            if let Some(_) = self.store.find_quad(&d.quad)? {
                                return Err("ErrQuadExists".into())
                            }
                        }
                    },
                    Procedure::Delete => {
                        if !ignore_opts.ignore_missing {
                            if let Some(_) = self.store.find_quad(&d.quad)? {
                            } else {
                                return Err("ErrQuadNotExist".into())
                            }
                        }
                    },
                }
            }
        }

        for d in &deltas {
            match &d.action {
                Procedure::Add => {
                    self.store.add_quad(d.quad.clone())?;
                },
                Procedure::Delete => {
                   if let Some(id) = self.store.find_quad(&d.quad)? {
                    self.store.delete(id)?;
                   }
                }
            }
        }

        Ok(())
    }

    fn nodes_all_iterator(&self) -> Rc<RefCell<dyn Shape>> {
        RocksDbAllIterator::new(self.store.clone(), true)
  
    }

    fn quads_all_iterator(&self) -> Rc<RefCell<dyn Shape>> {
        RocksDbAllIterator::new(self.store.clone(), false)
    }

    fn close(&self) -> Option<String> {
        // TODO: how to close the RocksDB, destroy()?
        return None
    }
}

pub struct PrimitiveCount {
    values: u64,
    quads: u64
}

impl PrimitiveCount {
    fn zero() -> PrimitiveCount {
        PrimitiveCount {
            values: 0,
            quads: 0
        }
    }

    fn decode(bytes: &[u8]) -> PrimitiveCount {
        let values = BigEndian::read_u64(&bytes[0..8]);
        let quads = BigEndian::read_u64(&bytes[8..16]);

        PrimitiveCount {
            values,
            quads
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut v:Vec<u8> = Vec::new();

        v.write_u64::<BigEndian>(self.values).unwrap();
        v.write_u64::<BigEndian>(self.quads).unwrap();
    
        v
    }

    pub fn total(&self) -> u64 {
        return self.values + self.quads
    }

    fn increment_quads(&mut self, n: i64) {
        if n < 0 {
            let m = n.abs() as u64;
            if m > self.quads {
                // return Err("Attempted to set quad count to less than 0".to_string());
            } else {
                self.quads -= m;
            }
        } else {
            if n as u64 > u64::max_value() - self.quads  {
                // return Err("quad count is invalid u64::max_value()".to_string());
            } else {
                self.quads += n as u64;
            }
        }
    }

    fn increment_values(&mut self, n: i64) {
        if n < 0 {
            let m = n.abs() as u64;
            if m > self.values {
                // return Err("Attempted to set quad count to less than 0".to_string());
            } else {
                self.values -= m;
            }
        } else {
            if n as u64 > u64::max_value() - self.values  {
                // return Err("quad count is invalid u64::max_value()".to_string());
            } else {
                self.values += n as u64;
            }
        }
    }
}

// TODO: eliminate the use of cursor with BigEndian::read_u64(&buf)

pub const QUAD_DIRECTION_KEY_PREFIX:u8 = 2u8;
pub const ID_HASH_INDEX_PREFIX:u8 = 1u8;
pub const PRIMITIVE_KEY_PREFIX:u8 = 0u8; // should always be 0 so when can interate primitives using IteratorMode::Start
pub const PRIMITIVE_COUNT_KEY:u8 = 255;


fn id_hash_key(hash: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.push(ID_HASH_INDEX_PREFIX);
    v.write_u64::<BigEndian>(hash).unwrap();

    v
}


fn quad_direction_key(value_id: u64, direction: &Direction, quad_id: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.push(QUAD_DIRECTION_KEY_PREFIX);
    v.push(direction.to_byte());
    v.write_u64::<BigEndian>(value_id).unwrap();
    v.write_u64::<BigEndian>(quad_id).unwrap();

    v
}

fn decode_quad_direction_key(bytes: &[u8]) -> (u64, Direction, u64) {

    let mut pos:usize = 1; // ignore QUAD_DIRECTION_KEY_PREFIX

    let direction = Direction::from_byte(bytes[pos]).unwrap();
    pos += 1;

    let mut rdr = Cursor::new(&bytes[pos..pos+8]);
    let value_id = rdr.read_u64::<BigEndian>().unwrap();
    pos += 8;

    let mut rdr = Cursor::new(&bytes[pos..pos+8]);
    let quad_id = rdr.read_u64::<BigEndian>().unwrap();

    (value_id, direction, quad_id)

}


pub fn primitive_key(id: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.push(PRIMITIVE_KEY_PREFIX);
    v.write_u64::<BigEndian>(id).unwrap();

    v
}

// fn primitive_key_from_value(id: u64) -> Vec<u8> {
//     let mut v:Vec<u8> = Vec::new();

//     v.extend_from_slice("prim".as_bytes());
//     v.write_u64::<BigEndian>(id).unwrap();

//     v
// }


#[derive(Clone, PartialEq, Debug)]
pub struct Primitive {
    id: u64,
    refs: u64,
    pub content: PrimitiveContent
}


impl Primitive {

    fn calc_hash(&self) -> u64 {
        match &self.content {
            PrimitiveContent::Value(v) => {
                return v.calc_hash()
            },
            PrimitiveContent::InternalQuad(q) => {
                return q.calc_hash()
            },
        }
    }

    pub fn to_ref(&self, nodes: bool) -> Option<Ref> {

        match &self.content {
            PrimitiveContent::Value(v) => {
                if nodes {
                    return Some(Ref {
                        k: Some(self.id),
                        content: Content::Value(v.clone())
                    });
                }
            },
            PrimitiveContent::InternalQuad(q) => {
                if !nodes {
                    return Some(Ref {
                        k: Some(self.id),
                        content: Content::InternalQuad(q.clone())
                    });
                }
            }
        }

        return None
    }

    pub fn is_quad(&self) -> bool {
        if let PrimitiveContent::InternalQuad(_) = self.content {
            return true
        }

        return false
    }

    pub fn is_node(&self) -> bool {
        if let PrimitiveContent::Value(_) = self.content {
            return true
        }

        return false
    }

    pub fn new_value(v: Value) -> Primitive {
        let pc = PrimitiveContent::Value(v);
        Primitive {
            id: 0,
            content: pc,
            refs: 0
        }
    }

    pub fn new_quad(q: InternalQuad) -> Primitive {
        let pc = PrimitiveContent::InternalQuad(q);

        Primitive {
            id: 0,
            content: pc,
            refs: 0
        }
    }

    fn new(content: PrimitiveContent) -> Primitive {
        Primitive {
            id: 0,
            refs: 0,
            content
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut v:Vec<u8> = Vec::new();

        v.write_u64::<BigEndian>(self.id).unwrap();
        v.write_u64::<BigEndian>(self.refs).unwrap();
        self.content.encode(&mut v);

        v
    }

    pub fn decode(bytes: &[u8]) -> Result<Primitive, String> {

        let mut pos:usize = 0;

        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let id = rdr.read_u64::<BigEndian>().unwrap();
        pos += 8;

        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let refs = rdr.read_u64::<BigEndian>().unwrap();
        pos += 8;
        
        let content = PrimitiveContent::decode(&bytes[pos..])?;

        Ok(Primitive {
            id,
            refs,
            content
        })
    }
}

#[derive(Clone, PartialEq, Debug, Hash)]
pub enum PrimitiveContent {
    Value(Value),
    InternalQuad(InternalQuad)
}


const PRIMITIVE_CONTENT_VALUE_PREFIX:u8 = 0u8;
const PRIMITIVE_CONTENT_QUAD_PREFIX:u8 = 1u8;


impl PrimitiveContent {


    fn encode(&self, buff: &mut Vec<u8>){
        match self {
            PrimitiveContent::Value(v) => {
                buff.push(PRIMITIVE_CONTENT_VALUE_PREFIX);
                v.encode(buff);
            },
            PrimitiveContent::InternalQuad(q) => {
                buff.push(PRIMITIVE_CONTENT_QUAD_PREFIX);
                q.encode(buff);
            },
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<PrimitiveContent, String> {
        if bytes[0] == PRIMITIVE_CONTENT_VALUE_PREFIX {
            return Ok(PrimitiveContent::Value(Value::decode(&bytes[1..])?));
        } else if bytes[0] == PRIMITIVE_CONTENT_QUAD_PREFIX {
            return Ok(PrimitiveContent::InternalQuad(InternalQuad::decode(&bytes[1..])?));
        } 
            
        Err("Cannot not decode PrimitiveContent".to_string())
    }
}



#[test]
fn primitive_key_tests() {
    assert_eq!(primitive_key(256), vec![0, 0, 0, 0, 0, 0, 0, 1, 0]);
    assert_eq!(primitive_key(u64::max_value()), vec![0, 255, 255, 255, 255, 255, 255, 255, 255]);
}

#[test]
fn primitive_quad_encoding_tests() {
    let p1 = Primitive::new_quad(InternalQuad {
        s: 0,
        p: 1,
        o: 2,
        l: 3
    });
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);

    let p1 = Primitive::new_quad(InternalQuad {
        s: 4234664324,
        p: 22345353643,
        o: 1686585436346,
        l: 0
    });
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
}


#[test]
fn primitive_value_encoding_tests() {
    let p1 = Primitive::new_value("Foo Bar".into());
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.calc_hash(), p2.calc_hash());

    let p1 = Primitive::new_value(747.into());
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.calc_hash(), p2.calc_hash());

    let p1 = Primitive::new_value("<foo>".into());
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.calc_hash(), p2.calc_hash());

    let p1 = Primitive::new_value(Value::None);
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.calc_hash(), p2.calc_hash());

    let p1 = Primitive::new_value(false.into());
    let e = p1.encode();
    let p2 = Primitive::decode(&e).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(p1.calc_hash(), p2.calc_hash());
}

#[test]
fn quad_direction_key_tests() {
    let t1 = (0, Direction::Subject, 0);
    let b = quad_direction_key(t1.0, &t1.1, t1.2);
    let t2 = decode_quad_direction_key(&b);
    assert_eq!(t1, t2);

    let t1 = (123, Direction::Object, 321);
    let b = quad_direction_key(t1.0, &t1.1, t1.2);
    let t2 = decode_quad_direction_key(&b);
    assert_eq!(t1, t2);

    let t1 = (u64::max_value(), Direction::Label, u64::max_value());
    let b = quad_direction_key(t1.0, &t1.1, t1.2);
    let t2 = decode_quad_direction_key(&b);
    assert_eq!(t1, t2);
}


#[test]
fn internal_rocks_db_tests() {
    let db = InternalRocksDB::open("gizmo_tests.db").unwrap();

    let mut p1 = Primitive::new_value("<foo>".into());

    let mut p2 = Primitive::new_quad(InternalQuad {
        s: 4234664324,
        p: 22345353643,
        o: 1686585436346,
        l: 0
    });

    let mut p3 = Primitive::new_value(747.into());


    let id1 = db.add_primitive(&mut p1).unwrap();
    assert_eq!(id1, p1.id);

    let id2 = db.add_primitive(&mut p2).unwrap();
    assert_eq!(id2, p2.id);

    let id3 = db.add_primitive(&mut p3).unwrap();
    assert_eq!(id3, p3.id);

    let p1b = db.get_primitive(id1).unwrap().unwrap();
    assert_eq!(p1, p1b);

    let p2b = db.get_primitive(id2).unwrap().unwrap();
    assert_eq!(p2, p2b);

    let p3b = db.get_primitive(id3).unwrap().unwrap();
    assert_eq!(p3, p3b);


    db.remove_primitive(&p1b).unwrap();
    db.remove_primitive(&p2b).unwrap();
    db.remove_primitive(&p3b).unwrap();

    let id = db.get_primitive(id1).unwrap();
    assert!(id.is_none());

    let id = db.get_primitive(id2).unwrap();
    assert!(id.is_none());

    let id = db.get_primitive(id3).unwrap();
    assert!(id.is_none());
}