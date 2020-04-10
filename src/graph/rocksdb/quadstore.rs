use crate::graph::value::Value;
use crate::graph::refs::{Size, Ref, Namer, Content};
use crate::graph::iterator::{Shape, Null};
use crate::graph::quad::{QuadStore, Quad, Direction, Stats, Delta, IgnoreOptions, Procedure};

use std::rc::Rc;
use std::cell::RefCell;

use rocksdb::{DB, IteratorMode, Options};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

use std::collections::BTreeSet;

use std::sync::Arc;

pub struct InternalRocksDB {
    db: DB
}

impl InternalRocksDB {
    fn open(path: String) -> Result<InternalRocksDB, String> {
        Ok(InternalRocksDB {
            db: match DB::open_default(path) {
                Ok(db) => db,
                Err(_) => return Err("Unable to open database file".to_string())
            }
        })
    }


    // Primitives

    fn get_primitive(&self, id: u64) -> Result<Option<Primitive>, String> {
        let key = primitive_key(id);

        match self.db.get(key) {
            Ok(Some(pr)) => Ok(Some(Primitive::decode(&pr)?)),
            Ok(None) => Ok(None),
            Err(e) => Err("read/write error encountered".to_string())
        }
    }

    fn add_primitive(&self, mut p: Primitive) -> Result<u64, String> {
        p.refs = 1;
        self.db.put(primitive_key(p.id), p.encode());
        Ok(p.id)
    }
    

    // Quad Direction Index

    fn remove_primitive(&self, id: u64) -> Result<(), String> {
        self.db.delete(primitive_key(id))?;
        Ok(())
    }



    fn get_quad_direction(&self, d: &Direction, value_id: &u64) -> BTreeSet<u64> {
        let lower_bound = quad_direction_key(value_id.clone(), d, 0);
        self.db.iterator(IteratorMode::From(&lower_bound, rocksdb::Direction::Forward)).map(|(k,_)| decode_quad_direction_key(k).2).collect()
    }

    fn add_quad_direction(&self, value_id: u64, direction: &Direction, quad_id: u64) -> Result<(), String> {
        self.db.put(quad_direction_key(value_id, direction, quad_id), [])?;
        Ok(())
    }

    fn remove_quad_direction(&self, value_id: u64, direction: &Direction, quad_id: u64) -> Result<(), String> {
        self.db.delete(quad_direction_key(value_id, direction, quad_id))?;
        Ok(())
    }


    ///////////////////////


    fn resolve_val(&self, v: &Value, add: bool) -> Result<Option<u64>, String> {
        if let Value::None = v {
            return Ok(None)
        }

        let id = v.calc_hash();
        
        let prim = self.get_primitive(id)?;

        
        if prim.is_some() || !add {
            // if the value exsists and we are adding it, increment refs
            let res = prim.as_ref().map(|p| p.id);
            
            if prim.is_some() && add {
                let mut p = prim.unwrap();
                p.refs += 1;
                self.db.put(primitive_key(p.id), p.encode());
            }
            
            return Ok(res)
        }

        self.add_primitive(Primitive::new_value(v.clone()))?;

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
            return Ok(Some(q.calc_hash()))
        }
        Ok(None)
    }


    fn delete_quad_nodes(&self, q: &InternalQuad) -> Result<(), String> {
        for dir in Direction::iterator() {
            let id = q.dir(dir);
            if id == 0 {
                continue
            }

            let mut delete = false;

            if let Some(mut p) = self.get_primitive(id)? {

                p.refs -= 1;

                if p.refs < 0 {

                    panic!("remove of delete node");

                } else if p.refs == 0 {

                    self.remove_primitive(id)?;

                } else {

                    if let Err(_) = self.db.put(primitive_key(id), p.encode()) {
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
            if let PrimitiveContent::Quad(q) = p.content {
                quad = Some(q.clone());
            }
        } else {
            return Ok(false)
        }
        
        self.remove_primitive(id)?;
        
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
        let id = p.calc_hash();


        let p = self.get_primitive(id)?;

        // if prim already exsits
        if p.is_some() {
            return Ok(id)
        }

        // get value_ids for each direction, this time inserting the values as neccecery
        let p = self.resolve_quad_default(&q, true)?;

        // add value primitive
        let pr = Primitive::new_quad(p.clone());
        let id = self.add_primitive(pr)?;

        // add to index
        for d in Direction::iterator() {
            self.add_quad_direction(p.dir(d), d, id);
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
                    PrimitiveContent::Quad(q) => Ok(Some(q)),
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
    pub fn open(path: String) -> Result<RocksDB, String> {
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
        let id = v.calc_hash();
        Some(Ref {
            k: Some(id),
            content: Content::Value(v.clone())
        })
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
        let quad_res = self.store.internal_quad(r);
        if let Ok(quad) = quad_res {
            match quad {
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
        } else {
            // TODO: return Err
            return None
        }
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
        //
    }
}



const QUAD_DIRECTION_KEY_PREFIX:u8 = 1u8;
const PRIMITIVE_KEY_PREFIX:u8 = 0u8;

fn quad_direction_key(value_id: u64, direction: &Direction, quad_id: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.push(QUAD_DIRECTION_KEY_PREFIX);
    v.push(direction.to_byte());
    v.write_u64::<BigEndian>(value_id).unwrap();
    v.write_u64::<BigEndian>(quad_id).unwrap();

    v
}

fn decode_quad_direction_key(bytes: Box<[u8]>) -> (u64, u8, u64) {

    let mut pos:usize = 0;

    let direction = bytes[pos];
    pos += 1;

    let mut rdr = Cursor::new(&bytes[pos..pos+8]);
    let value_id = rdr.read_u64::<BigEndian>().unwrap();
    pos += 8;

    let mut rdr = Cursor::new(&bytes[pos..pos+8]);
    let quad_id = rdr.read_u64::<BigEndian>().unwrap();

    (value_id, direction, quad_id)

}


fn primitive_key(id: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.push(PRIMITIVE_KEY_PREFIX);
    v.write_u64::<BigEndian>(id).unwrap();

    v
}

fn primitive_key_from_value(id: u64) -> Vec<u8> {
    let mut v:Vec<u8> = Vec::new();

    v.extend_from_slice("prim".as_bytes());
    v.write_u64::<BigEndian>(id).unwrap();

    v
}



struct Primitive {
    id: u64,
    refs: u64,
    content: PrimitiveContent
}


impl Primitive {

    pub fn is_quad(&self) -> bool {
        if let PrimitiveContent::Quad(_) = self.content {
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
        Primitive {
            id: 0,
            content: PrimitiveContent::Value(v),
            refs: 0
        }
    }

    pub fn new_quad(q: InternalQuad) -> Primitive {
        Primitive {
            id: 0,
            content: PrimitiveContent::Quad(q),
            refs: 0
        }
    }

    fn new(content: PrimitiveContent) -> Primitive {
        Primitive {
            id: content.calc_hash(),
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


#[derive(Debug, Hash)]
pub enum PrimitiveContent {
    Value(Value),
    Quad(InternalQuad)
}


const PRIMITIVE_CONTENT_VALUE_PREFIX:u8 = 0u8;
const PRIMITIVE_CONTENT_QUAD_PREFIX:u8 = 1u8;


impl PrimitiveContent {
    fn calc_hash(&self) -> u64 {
        match self {
            PrimitiveContent::Value(v) => {
                return v.calc_hash()
            },
            PrimitiveContent::Quad(q) => {
                return q.calc_hash()
            },
        }
    }

    fn encode(&self, buff: &mut Vec<u8>){
        match self {
            PrimitiveContent::Value(v) => {
                buff.push(PRIMITIVE_CONTENT_VALUE_PREFIX);
                v.encode(buff);
            },
            PrimitiveContent::Quad(q) => {
                buff.push(PRIMITIVE_CONTENT_QUAD_PREFIX);
                q.encode(buff);
            },
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<PrimitiveContent, String> {
        if bytes[0] == PRIMITIVE_CONTENT_VALUE_PREFIX {
            return Ok(PrimitiveContent::Value(Value::decode(&bytes[1..])?));
        } else if bytes[0] == PRIMITIVE_CONTENT_QUAD_PREFIX {
            return Ok(PrimitiveContent::Quad(InternalQuad::decode(&bytes[1..])?));
        } 
            
        Err("Cannot not decode PrimitiveContent".to_string())
    }
}



#[derive(PartialEq, Hash, Clone, Debug)]
pub struct InternalQuad {
    s: u64,
    p: u64,
    o: u64,
    l: u64,
}

impl Eq for InternalQuad {}

impl InternalQuad {

    fn calc_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    fn encode(&self, v: &mut Vec<u8>){
        v.write_u64::<BigEndian>(self.s).unwrap();
        v.write_u64::<BigEndian>(self.p).unwrap();
        v.write_u64::<BigEndian>(self.o).unwrap();
        v.write_u64::<BigEndian>(self.l).unwrap();
    }


    pub fn decode(bytes: &[u8]) -> Result<InternalQuad, String> {
        let mut pos:usize = 0;

        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let s = rdr.read_u64::<BigEndian>().unwrap();
        
        pos += 8;
        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let p = rdr.read_u64::<BigEndian>().unwrap();
        
        pos += 8;
        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let o = rdr.read_u64::<BigEndian>().unwrap();
        
        pos += 8;
        let mut rdr = Cursor::new(&bytes[pos..pos+8]);
        let l = rdr.read_u64::<BigEndian>().unwrap();

        Ok(InternalQuad {
            s,
            p,
            o,
            l
        })
    }


    fn dir(&self, dir: &Direction) -> u64 {
        match dir {
            Direction::Subject => self.s,
            Direction::Predicate => self.p,
            Direction::Object => self.o,
            Direction::Label => self.l
        }
    }


    fn set_dir(&mut self, dir: &Direction, vid: u64) {
        match dir {
            Direction::Subject => self.s = vid,
            Direction::Predicate => self.p = vid,
            Direction::Object => self.o = vid,
            Direction::Label => self.l = vid,
        };
    }
}