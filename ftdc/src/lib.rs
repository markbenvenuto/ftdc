use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::fs::File;
use std::io::Read;

// use byteorder::{LittleEndian, ReadBytesExt};

use bson::Bson;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use libflate::zlib::{Decoder, Encoder};
use std::io::Cursor;
use std::rc::Rc;

pub struct BSONMetricsWriter {
    // samples: usize,
    metrics: usize,
    metric_vec: Vec<Vec<i64>>,
    ref_doc: Document,
    // blocks : Vec<Vec<u8>>,
}

impl BSONMetricsWriter {
    pub fn new() -> BSONMetricsWriter {
        BSONMetricsWriter {
            // samples : 0,
            metrics: 0,
            metric_vec: Vec::new(),
            ref_doc: Document::new(),
            // blocks: Vec::new()
        }
    }

    // TODO - report if new block was started
    pub fn add_doc(&mut self, doc: &Document) {
        let mut met_vec = Vec::new();
        extract_metrics_int(doc, &mut met_vec);

        // first document
        if (self.ref_doc.is_empty()) {
            self.ref_doc = doc.clone();

            self.metric_vec.clear();
            self.metrics = met_vec.len();
            // self.samples = 0;

            // self.metric_vec.push(met_vec);
            return;
        }

        // If metric count the same?
        if self.metrics == met_vec.len() {
            self.metric_vec.push(met_vec);
        } else {
            self.flush_block();

            self.ref_doc = doc.clone();

            self.metric_vec.clear();
            self.metrics = met_vec.len();
        }
    }

    fn reset_block(&mut self) {}

    ///
    /// Format
    /// i32 littlendian
    /// zlib_block
    ///
    /// zlib_block
    /// i32 metric
    /// i32 sample
    /// bytes block
    pub fn flush_block(&mut self) {
        // TODO - compress block
        let mut uncompressed_block: Vec<u8> = Vec::new();
        uncompressed_block.reserve(4 + 4 + self.metrics * self.metric_vec.len());

        uncompressed_block.write_i32::<LittleEndian>(self.metrics as i32);
        uncompressed_block.write_i32::<LittleEndian>(self.metric_vec.len() as i32);

        // TODO
        // Compress

        // let mut encoder = Encoder::new(Vec::new());
        // encoder.write_all()
        // let encoded_data = encoder.finish().into_result().unwrap();
    }
}

pub struct BSONBlockReader {
    reader: BufReader<File>,
}

pub enum RawBSONBlock {
    Metadata(Document),
    Metrics(Document),
}

impl BSONBlockReader {
    pub fn new(file_name: &str) -> BSONBlockReader {
        let ff = File::open(file_name).unwrap();

        BSONBlockReader {
            reader: BufReader::new(ff),
        }
    }
}

// #[derive(Serialize, Deserialize, Debug)]
// pub struct MetadataDoc {
//     #[serde(rename = "_id")]  // Use MongoDB's special primary key field name when serializing
//     pub id: Date,
//     pub type: i32,
//     pub age: i32
// }

impl Iterator for BSONBlockReader {
    // add code here
    type Item = RawBSONBlock;

    fn next(&mut self) -> Option<RawBSONBlock> {
        let mut size_buf: [u8; 4] = [0, 0, 0, 0];

        let result = self.reader.read_exact(&mut size_buf);
        if result.is_err() {
            return None;
        }

        // todo: size == 4
        let mut size_rdr = Cursor::new(size_buf);
        let size = size_rdr.read_i32::<LittleEndian>().unwrap();
        println!("size2 {}", size);
        // // Look for the first 4 bytes
        // let mut rdr = Cursor::new(self.buffer);
        // // Note that we use type parameters to indicate which kind of byte order
        // // we want!
        // println!("size {}", size);

        let read_size = size as usize;
        let mut v: Vec<u8> = Vec::with_capacity(read_size);

        v.write_i32::<LittleEndian>(size).unwrap();

        v.resize(read_size, 0);
        let result = self.reader.read_exact(&mut v[4..]);
        if result.is_err() {
            return None;
        }

        println!("size3 {}", v.len());

        let doc: Document = bson::from_reader(&mut Cursor::new(&v)).unwrap();

        let ftdc_type = doc.get_i32("type").unwrap();

        if ftdc_type == 0 {
            return Some(RawBSONBlock::Metadata(doc));
        }

        Some(RawBSONBlock::Metrics(doc))
    }
}

fn extract_metrics_bson_int(value: &Bson, metrics: &mut Vec<i64>) {
    match value {
        &Bson::Double(f) => {
            metrics.push(f as i64);
        }
        &Bson::Int64(f) => {
            metrics.push(f);
        }
        &Bson::Int32(f) => {
            metrics.push(f as i64);
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Boolean(f) => {
            metrics.push(f as i64);
        }
        &Bson::DateTime(f) => {
            metrics.push(f.timestamp_millis() as i64);
        }
        &Bson::Timestamp(f) => {
            metrics.push(f.time as i64);
            metrics.push(f.increment as i64);
        }
        Bson::Document(o) => {
            extract_metrics_int(o, metrics);
        }
        Bson::Array(a) => {
            for b in a {
                extract_metrics_bson_int(b, metrics);
            }
        }

        &Bson::JavaScriptCode(_) => {}
        &Bson::JavaScriptCodeWithScope(_) => {}
        &Bson::Binary(_) => {}
        &Bson::ObjectId(_) => {}
        &Bson::DbPointer(_) => {}
        &Bson::MaxKey | &Bson::MinKey | &Bson::Undefined => {}
        &Bson::String(_) | &Bson::Null | &Bson::Symbol(_) | &Bson::RegularExpression(_) => {}
    }
}

fn extract_metrics_int(doc: &Document, metrics: &mut Vec<i64>) {
    for item in doc {
        let value = item.1;

        extract_metrics_bson_int(value, metrics);
    }
}

pub fn extract_metrics(doc: &Document) -> Vec<i64> {
    let mut metrics: Vec<i64> = Vec::new();
    extract_metrics_int(doc, &mut metrics);
    metrics
}

fn concat2(a1: &str, a2: &str) -> String {
    let mut s = a1.to_string();
    s.push_str(a2);
    s
}

fn concat3(a1: &str, a2: &str, a3: &str) -> String {
    let mut s = a1.to_string();
    s.push_str(a2);
    s.push('.');
    s.push_str(a3);
    s
}

fn extract_metrics_paths_bson_int(
    value: &(&String, &Bson),
    prefix: &str,
    metrics: &mut Vec<String>,
) {
    let prefix_dot_str = prefix.to_string() + ".";
    let prefix_dot = prefix_dot_str.as_str();
    let name = &value.0;
    match value.1 {
        &Bson::Double(_)
        | &Bson::Int64(_)
        | &Bson::Int32(_)
        | &Bson::Boolean(_)
        | &Bson::DateTime(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(a1);
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Timestamp(_) => {
            metrics.push(concat3(prefix_dot, name.as_str(), "t"));
            metrics.push(concat3(prefix_dot, name.as_str(), "i"));
        }
        Bson::Document(o) => {
            extract_metrics_paths_int(o, concat2(prefix_dot, name.as_str()).as_str(), metrics);
        }
        Bson::Array(a) => {
            for b in a {
                extract_metrics_paths_bson_int(
                    &(&name, b),
                    concat2(prefix_dot, name.as_str()).as_str(),
                    metrics,
                );
            }
        }

        &Bson::JavaScriptCode(_) => {}
        &Bson::JavaScriptCodeWithScope(_) => {}
        &Bson::Binary(_) => {}
        &Bson::ObjectId(_) => {}
        &Bson::DbPointer(_) => {}
        &Bson::MaxKey | &Bson::MinKey | &Bson::Undefined => {}

        &Bson::String(_) | &Bson::Null | &Bson::Symbol(_) | &Bson::RegularExpression(_) => {}
    }
}

fn extract_metrics_paths_int(doc: &Document, prefix: &str, metrics: &mut Vec<String>) {
    for item in doc {
        extract_metrics_paths_bson_int(&item, prefix, metrics);
    }
}

pub fn extract_metrics_paths(doc: &Document) -> Vec<String> {
    let mut metrics: Vec<String> = Vec::new();
    extract_metrics_paths_int(doc, "", &mut metrics);
    metrics
}

fn fill_to_bson_int(ref_field: (&String, &Bson), it: &mut dyn Iterator<Item = &u64>) -> Bson {
    match ref_field.1 {
        &Bson::Double(_) => Bson::Double(*it.next().unwrap() as f64),
        &Bson::Int64(_) => Bson::Int64(*it.next().unwrap() as i64),
        &Bson::Int32(_) => Bson::Int32(*it.next().unwrap() as i32),
        &Bson::Boolean(_) => Bson::Boolean(*it.next().unwrap() != 0),
        &Bson::DateTime(_) => {
            let p1 = it.next().unwrap();

            Bson::DateTime(bson::DateTime::from_millis(*p1 as i64))
        }
        Bson::Timestamp(_) => {
            let p1 = it.next().unwrap();
            let p2 = it.next().unwrap();

            Bson::Timestamp(bson::Timestamp {
                time: *p1 as u32,
                increment: *p2 as u32,
            })
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        Bson::Document(o) => {
            let mut doc_nested = Document::new();
            for ref_field2 in o {
                fill_document_bson_int(ref_field2, it, &mut doc_nested);
            }
            Bson::Document(doc_nested)
        }
        Bson::Array(a) => {
            let mut arr: Vec<Bson> = Vec::new();

            let c = "ignore".to_string();

            for &ref b in a {
                let tuple = (&c, b);
                arr.push(fill_to_bson_int(tuple, it));
            }

            Bson::Array(arr)
        }

        Bson::JavaScriptCode(a) => Bson::JavaScriptCode(a.to_string()),
        Bson::JavaScriptCodeWithScope(a) => Bson::JavaScriptCodeWithScope(a.clone()),
        Bson::Binary(a) => Bson::Binary(a.clone()),
        Bson::ObjectId(a) => Bson::ObjectId(*a),
        Bson::String(a) => Bson::String(a.to_string()),
        &Bson::Null => Bson::Null,
        Bson::Symbol(a) => Bson::Symbol(a.to_string()),
        Bson::RegularExpression(a) => Bson::RegularExpression(a.clone()),
        Bson::DbPointer(a) => Bson::DbPointer(a.clone()),
        &Bson::MaxKey => Bson::MaxKey,
        &Bson::MinKey => Bson::MinKey,
        &Bson::Undefined => Bson::Undefined,
    }
}

fn fill_document_bson_int(
    ref_field: (&String, &Bson),
    it: &mut dyn Iterator<Item = &u64>,
    doc: &mut Document,
) {
    doc.insert(ref_field.0.to_string(), fill_to_bson_int(ref_field, it));
}

pub fn fill_document(ref_doc: &Document, metrics: &[u64]) -> Document {
    let mut doc = Document::new();

    let mut cur = metrics.iter();

    for item in ref_doc {
        fill_document_bson_int(item, &mut cur, &mut doc);
    }

    doc
}

// pub enum MetricsDocument<'a> {
//     Reference(&'a Document),
//     Metrics(Vec<i64>),
// }

// pub enum MetricsDocument<'a> {
//     Reference(&'a Box<Document>),
//     Metrics(Document),
//     // Metrics(&'a [i64]),
// }

#[derive(Debug)]
pub enum MetricsDocument {
    Reference(Rc<Document>),
    Metrics(Document),
}

enum MetricState {
    Reference,
    Metrics,
}

pub struct MetricsReader<'a> {
    doc: &'a Document,
    ref_doc: Rc<Document>,
    //data: Vec<i64>,
    it_state: MetricState,
    sample: i32,
    sample_count: i32,
    raw_metrics: Vec<u64>,
    metrics_count: i32,

    decoded_data: Vec<u8>,

    cursor: Option<Cursor<&'a Vec<u8>>>,
}

impl<'a> MetricsReader<'a> {
    pub fn new<'b>(doc: &'b Document) -> MetricsReader<'b> {
        MetricsReader {
            doc,
            ref_doc: Rc::new(Document::new()),
            it_state: MetricState::Reference,
            sample: 0,
            sample_count: 0,
            metrics_count: 0,
            decoded_data: Vec::new(),
            raw_metrics: Vec::new(),
            cursor: None,
        }
    }

    /*pub fn decode(&'a mut self) {
        let blob = self.doc.get_binary_generic("data").unwrap();

        let mut size_rdr = Cursor::new(&blob);
        let un_size = size_rdr.read_i32::<LittleEndian>().unwrap();
        println!("Uncompressed size {}", un_size);

        // skip the length in the compressed blob
        let mut decoder = Decoder::new(&blob[4..]).unwrap();
        decoder.read_to_end(&mut self.decoded_data).unwrap();

        self.cursor = Some(Cursor::new(&self.decoded_data));
        self.ref_doc = Rc::new(bson::from_reader(&mut self.cursor.as_mut().unwrap()).unwrap());

        let metric_count = self
            .cursor
            .as_mut()
            .unwrap()
            .read_i32::<LittleEndian>()
            .unwrap();
        println!("metric_count {}", metric_count);

        self.sample_count = self
            .cursor
            .as_mut()
            .unwrap()
            .read_i32::<LittleEndian>()
            .unwrap();
        println!("sample_count {}", self.sample_count);

        // Extract metrics from reference document
        // let ref_metrics = extract_metrics(&self.ref_doc);

        // Decode metrics
        self.raw_metrics
            .reserve((metric_count * self.sample_count) as usize);

        // TODO: Don't decode all metrics initially
        // let mut val: u64 = 0;
        // for _ in 0..q.sample_count {
        //     for _ in 0..metric_count {
        //         let read_size = decode(q.cursor.get_ref(), &mut val);
        //         q.cursor.consume(read_size);
        //         q.raw_metrics.push(val);
        //     }
        // }
    }*/
}

impl<'a> Iterator for MetricsReader<'a> {
    type Item = MetricsDocument;

    fn next(&mut self) -> Option<MetricsDocument> {
        if self.raw_metrics.is_empty() {
            let blob = self.doc.get_binary_generic("data").unwrap();

            let mut size_rdr = Cursor::new(&blob);
            let un_size = size_rdr.read_i32::<LittleEndian>().unwrap();
            println!("Uncompressed size {}", un_size);

            // skip the length in the compressed blob
            let mut decoder = Decoder::new(&blob[4..]).unwrap();
            decoder.read_to_end(&mut self.decoded_data).unwrap();

            let mut cur = Cursor::new(&self.decoded_data);
            self.ref_doc = Rc::new(bson::from_reader(&mut cur).unwrap());

            self.metrics_count = cur.read_i32::<LittleEndian>().unwrap();
            println!("metric_count {}", self.metrics_count);

            self.sample_count = cur.read_i32::<LittleEndian>().unwrap();
            println!("sample_count {}", self.sample_count);

            // Extract metrics from reference document
            //                let ref_metrics = extract_metrics(&self.ref_doc);

            // Decode metrics
            self.raw_metrics
                .reserve((self.metrics_count * self.sample_count) as usize);

            // TODO: Don't decode all metrics initially

            let mut zeros_count = 0;

            let mut pos: usize = cur.position() as usize;
            let buf = self.decoded_data.as_ref();

            for _ in 0..self.sample_count {
                for _ in 0..self.metrics_count {
                    if zeros_count > 0 {
                        self.raw_metrics.push(0);
                        zeros_count -= 1;
                        continue;
                    }

                    let mut val: u64 = 0;
                    let read_size = varinteger::decode_with_offset(buf, pos, &mut val);
                    pos += read_size;

                    if val == 0 {
                        // Read zeros count
                        let read_size = varinteger::decode_with_offset(buf, pos, &mut val);
                        pos += read_size;

                        zeros_count = val;
                    }

                    self.raw_metrics.push(val);
                }
            }

            assert_eq!(
                (self.sample_count * self.metrics_count) as usize,
                self.raw_metrics.len()
            );
        }

        match self.it_state {
            MetricState::Reference => {
                self.it_state = MetricState::Metrics;
                Some(MetricsDocument::Reference(self.ref_doc.clone()))
            }
            MetricState::Metrics => {
                // return None;
                if self.sample == self.sample_count {
                    return None;
                }
                self.sample += 1;

                let start_index: usize = ((self.sample - 1) * self.metrics_count) as usize;
                let end_index: usize = (self.sample * self.metrics_count) as usize;

                let d = fill_document(&self.ref_doc, &self.raw_metrics[start_index..end_index]);
                Some(MetricsDocument::Metrics(d))
                // return Some(&MetricsDocument::Metrics(self.raw_metrics[((self.sample - 1) * self.metrics_count)..(self.sample * self.metrics_count)]))
            }
        }
    }
}
