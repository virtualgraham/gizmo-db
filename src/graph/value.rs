use std::hash::{Hash, Hasher};
use std::borrow::Cow;
use std::fmt;
use super::number::Number;
use std::collections::hash_map::DefaultHasher;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use std::io::Cursor;


#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    None,
    Null,
    Bool(bool),
    Number(Number),
    IRI(String),
    String(String),
}

const VALUE_NONE_PREFIX:u8 = 0;
const VALUE_NULL_PREFIX:u8 = 1;
const VALUE_BOOL_TRUE_PREFIX:u8 = 2;
const VALUE_BOOL_FALSE_PREFIX:u8 = 3;
const VALUE_NUMBER_F64_PREFIX:u8 = 4;
const VALUE_NUMBER_I64_PREFIX:u8 = 5;
const VALUE_NUMBER_U64_PREFIX:u8 = 6;
const VALUE_IRI_PREFIX:u8 = 7;
const VALUE_STRING_PREFIX:u8 = 8;


impl Value {

    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Number(n) = self {
            return n.as_i64()
        }
        None
    }

    fn from_rdf_string<S: Into<String>>(s: S) -> Value {
        let s = s.into();
        if s.is_empty() {
            return Value::String(s)
        } else if s.starts_with('"') && s.ends_with('"') {
            let v = &s[1..s.len()-1];
            Value::String(v.to_string())
        } else if s.starts_with('<') && s.ends_with('>') {
            let v = &s[1..s.len()-1];
            Value::IRI(v.to_string())
        } else {
            Value::String(s)
        }
    }

    fn from_string<S: Into<String>>(s: S) -> Value {
        let s = s.into();
        if s.is_empty() {
            return Value::String(s)
        } else if s.starts_with('<') && s.ends_with('>') {
            let v = &s[1..s.len()-1];
            Value::IRI(v.to_string())
        } else {
            Value::String(s)
        }
    }
    
    pub fn calc_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
   
    // pub fn calc_hash_bytes(&self) -> [u8; 8] {
    //     let mut s = DefaultHasher::new();
    //     self.hash(&mut s);
    //     s.finish().to_be_bytes()
    // }

    pub fn encode(&self, buff: &mut Vec<u8>) {
        match self {
            Value::None => buff.push(VALUE_NONE_PREFIX),
            Value::Null => buff.push(VALUE_NULL_PREFIX),
            Value::Bool(b) => if *b { buff.push(VALUE_BOOL_TRUE_PREFIX) } else { buff.push(VALUE_BOOL_FALSE_PREFIX) },
            Value::Number(n) => {
                if n.is_f64() {
                    let i = n.as_f64().unwrap();
                    buff.push(VALUE_NUMBER_F64_PREFIX);
                    buff.write_f64::<BigEndian>(i);
                } else if n.is_u64() {
                    let i = n.as_u64().unwrap();
                    buff.push(VALUE_NUMBER_U64_PREFIX);
                    buff.write_u64::<BigEndian>(i);
                } else if n.is_i64() {
                    let i = n.as_i64().unwrap();
                    buff.push(VALUE_NUMBER_I64_PREFIX);
                    buff.write_i64::<BigEndian>(i);
                }
            },
            Value::IRI(s) => {
                buff.push(VALUE_IRI_PREFIX);
                buff.extend_from_slice(s.as_bytes());
            },
            Value::String(s) => {
                buff.push(VALUE_STRING_PREFIX);
                buff.extend_from_slice(s.as_bytes());
            },
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Value, String> {
        if bytes.is_empty() {
            return Err("Cannot decode value".to_string());
        }

        if bytes[0] == VALUE_NULL_PREFIX {
            return Ok(Value::Null)
        } else if bytes[0] == VALUE_BOOL_TRUE_PREFIX {
            return Ok(Value::Bool(true))
        } else if bytes[0] == VALUE_BOOL_FALSE_PREFIX {
            return Ok(Value::Bool(false))
        } else if bytes[0] == VALUE_NUMBER_F64_PREFIX {
            let mut rdr = Cursor::new(&bytes[1..]);
            let n = rdr.read_f64::<BigEndian>().unwrap();
            return Ok(Value::Number(Number::from_f64(n).unwrap()))
        } else if bytes[0] == VALUE_NUMBER_U64_PREFIX {
            let mut rdr = Cursor::new(&bytes[1..]);
            let n = rdr.read_u64::<BigEndian>().unwrap();
            return Ok(Value::Number(n.into()))
        } else if bytes[0] == VALUE_NUMBER_I64_PREFIX {
            let mut rdr = Cursor::new(&bytes[1..]);
            let n = rdr.read_i64::<BigEndian>().unwrap();
            return Ok(Value::Number(n.into()))
        } else if bytes[0] == VALUE_IRI_PREFIX {
            let s = std::str::from_utf8(&bytes[1..]).unwrap();
            return Ok(Value::IRI(s.into()))
        } else if bytes[0] == VALUE_STRING_PREFIX {
            let s = std::str::from_utf8(&bytes[1..]).unwrap();
            return Ok(Value::String(s.into()))
        } else {
            return Ok(Value::None)
        }
    }
}


impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::None => write!(f, "undefined"),
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Number(n) => write!(f, "{}", n),
            Value::IRI(s) => write!(f, "<{}>", s),
            Value::String(s) => write!(f, "{}", s),
        }
        
    }
}


impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::None => {
                "Value::Undefined".hash(state);
            },
            Value::Null => {
                "Value::Null".hash(state);
            },
            Value::Bool(b) => {
                "Value::Bool".hash(state);
                b.hash(state);
            },
            Value::Number(n) => {
                "Value::Number".hash(state);
                n.hash(state);
            },
            Value::IRI(s) => {
                "Value::IRI".hash(state);
                s.hash(state);
            },
            Value::String(s) => {
                "Value::String".hash(state);
                s.hash(state);
            }
        }
    }
}



macro_rules! from_integer {
    ($($ty:ident)*) => {
        $(
            impl From<$ty> for Value {
                fn from(n: $ty) -> Self {
                    Value::Number(n.into())
                }
            }
        )*
    };
}

from_integer! {
    i8 i16 i32 i64 isize
    u8 u16 u32 u64 usize
}


impl From<f32> for Value {
    /// Convert 32-bit floating point number to `Value`
    fn from(f: f32) -> Self {
        From::from(f as f64)
    }
}

impl From<f64> for Value {
    /// Convert 64-bit floating point number to `Value`
    fn from(f: f64) -> Self {
        Number::from_f64(f).map_or(Value::Null, Value::Number)
    }
}

impl From<bool> for Value {
    /// Convert boolean to `Value`
    fn from(f: bool) -> Self {
        Value::Bool(f)
    }
}

impl From<String> for Value {
    /// Convert `String` to `Value`
    fn from(f: String) -> Self {
        Value::from_string(f)
    }
}

impl<'a> From<&'a str> for Value {
    /// Convert string slice to `Value`
    fn from(f: &str) -> Self {
        Value::from_string(f)
    }
}

impl<'a> From<()> for Value {
    /// Convert string slice to `Value`
    fn from(_: ()) -> Self {
        Value::None
    }
}

impl<'a> From<Cow<'a, str>> for Value {
    /// Convert copy-on-write string to `Value`
    fn from(f: Cow<'a, str>) -> Self {
        Value::String(f.into_owned())
    }
}

// impl From<&JsValue> for Value {

//     fn from(f: &JsValue) -> Self {
//         if f.is_undefined() {
//             return Value::Undefined
//         } 

//         if f.is_null() {
//             return Value::Null
//         } 
        
//         if f.is_string() {
//             return match f.as_string() {
//                 Some(s) => Value::String(s),
//                 None => Value::Undefined
//             }
//         }

//         match f.as_f64() {
//             Some(n) => Value::from(n),
//             None => match f.as_bool() {
//                 Some(b) => Value::Bool(b),
//                 None => Value::Undefined
//             }
//         }
//     }

// }