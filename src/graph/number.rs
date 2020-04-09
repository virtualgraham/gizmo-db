use std::fmt;
use ordered_float::OrderedFloat;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};


#[derive(Clone, Eq, PartialEq)]
pub struct Number {
    n: N,
}

impl Number {
    pub fn is_i64(&self) -> bool {
        match self.n {
            N::PosInt(v) => v <= i64::max_value() as u64,
            N::NegInt(_) => true,
            N::Float(_) => false,
        }
    }

    pub fn is_u64(&self) -> bool {
        match self.n {
            N::PosInt(_) => true,
            N::NegInt(_) | N::Float(_) => false,
        }
    }

    pub fn is_f64(&self) -> bool {
        match self.n {
            N::Float(_) => true,
            N::PosInt(_) | N::NegInt(_) => false,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self.n {
            N::PosInt(n) => {
                if n <= i64::max_value() as u64 {
                    Some(n as i64)
                } else {
                    None
                }
            }
            N::NegInt(n) => Some(n),
            N::Float(_) => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self.n {
            N::PosInt(n) => Some(n),
            N::NegInt(_) | N::Float(_) => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self.n {
            N::PosInt(n) => Some(n as f64),
            N::NegInt(n) => Some(n as f64),
            N::Float(n) => Some(n.into()),
        }
    }

    pub fn from_f64(f: f64) -> Option<Number> {
        if f.is_finite() {
            Some(Number { n: N::Float(OrderedFloat(f)) })
        } else {
            None
        }
    }
}

impl fmt::Display for Number {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self.n {
            N::PosInt(u) => fmt::Display::fmt(&u, formatter),
            N::NegInt(i) => fmt::Display::fmt(&i, formatter),
            N::Float(f) => fmt::Display::fmt(&f, formatter),
        }
    }
}

impl Debug for Number {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let mut debug = formatter.debug_tuple("Number");
        match self.n {
            N::PosInt(i) => {
                debug.field(&i);
            }
            N::NegInt(i) => {
                debug.field(&i);
            }
            N::Float(f) => {
                debug.field(&f);
            }
        }
        debug.finish()
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum N {
    PosInt(u64),
    /// Always less than zero.
    NegInt(i64),
    /// Always finite.
    Float(OrderedFloat<f64>),
}






impl Eq for N {}


impl Hash for Number {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.n {
            N::PosInt(i) => {
                i.hash(state);
            }
            N::NegInt(i) => {
                i.hash(state);
            }
            N::Float(f) => {
                f.hash(state);
            }
        }
    }
}


macro_rules! impl_from_unsigned {
    (
        $($ty:ty),*
    ) => {
        $(
            impl From<$ty> for Number {
                #[inline]
                fn from(u: $ty) -> Self {
                    let n = {
                        { N::PosInt(u as u64) }
                    };
                    Number { n: n }
                }
            }
        )*
    };
}

macro_rules! impl_from_signed {
    (
        $($ty:ty),*
    ) => {
        $(
            impl From<$ty> for Number {
                #[inline]
                fn from(i: $ty) -> Self {
                    let n = {
                        {
                            if i < 0 {
                                N::NegInt(i as i64)
                            } else {
                                N::PosInt(i as u64)
                            }
                        }
                    };
                    Number { n: n }
                }
            }
        )*
    };
}

impl_from_unsigned!(u8, u16, u32, u64, usize);
impl_from_signed!(i8, i16, i32, i64, isize);