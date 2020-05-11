use std::{
    fs::File,
    io::{self, prelude::*},
    rc::Rc
};
use flate2::read::GzDecoder;
use crate::graph::quad::Quad;
use crate::graph::value::Value;

pub struct BufReader {
    reader: io::BufReader<GzDecoder<File>>,
    buf: Rc<String>,
}

fn new_buf() -> Rc<String> {
    Rc::new(String::with_capacity(1024)) // Tweakable capacity
}

impl BufReader {
    pub fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let gz = GzDecoder::new(file);
        let reader = io::BufReader::new(gz);
        let buf = new_buf();
        Ok(Self { reader, buf })
    }
}

impl Iterator for BufReader {
    type Item = io::Result<Quad>;

    fn next(&mut self) -> Option<Self::Item> {
        let buf = match Rc::get_mut(&mut self.buf) {
            Some(buf) => {
                buf.clear();
                buf
            }
            None => {
                self.buf = new_buf();
                Rc::make_mut(&mut self.buf)
            }
        };

        self.reader
            .read_line(buf)
            .map(|u| if u == 0 { None } else { Some(Rc::clone(&self.buf)) })
            .map(|o| o.map(|l| {
                let mut v_iter = l.split_whitespace().take_while(|s| *s != ".").map(|s| Value::from_rdf_string(s));
                Quad::new(
                    if let Some(v) = v_iter.next() { v } else { Value::None },
                    if let Some(v) = v_iter.next() { v } else { Value::None },
                    if let Some(v) = v_iter.next() { v } else { Value::None },
                    if let Some(v) = v_iter.next() { v } else { Value::None },
                )
            }))
            .transpose()
    }
}