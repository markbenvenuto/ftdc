// Copyright [2024] [Mark Benvenuto]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs::File;
use std::io::BufReader;
use std::io::Read;

use anyhow::Result;
use bson::Bson;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use libflate::zlib::{Decoder, Encoder};
use std::io::Cursor;
use std::rc::Rc;

pub struct BSONMetricsWriter {
    // samples: usize,
    metrics: usize,
    metric_vec: Vec<Vec<u64>>,
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
        if self.ref_doc.is_empty() {
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
    pub fn new(file_name: &str) -> Result<BSONBlockReader> {
        let ff = File::open(file_name)?;

        Ok(BSONBlockReader {
            reader: BufReader::new(ff),
        })
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
        // println!("size2 {}", size);
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

        // println!("size3 {}", v.len());
        // println!("Pos: {}\n", self.reader.stream_position().unwrap());

        let doc: Document = bson::from_reader(&mut Cursor::new(&v)).unwrap();

        let ftdc_type = doc.get_i32("type").unwrap();

        if ftdc_type == 0 {
            return Some(RawBSONBlock::Metadata(doc));
        }

        Some(RawBSONBlock::Metrics(doc))
    }
}

fn extract_metrics_bson_int(value: &Bson, metrics: &mut Vec<u64>) {
    match value {
        &Bson::Double(f) => {
            metrics.push(f as u64);
        }
        &Bson::Int64(f) => {
            metrics.push(f as u64);
        }
        &Bson::Int32(f) => {
            metrics.push(f as u64);
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Boolean(f) => {
            metrics.push(f as u64);
        }
        &Bson::DateTime(f) => {
            metrics.push(f.timestamp_millis() as u64);
        }
        &Bson::Timestamp(f) => {
            metrics.push(f.time as u64);
            metrics.push(f.increment as u64);
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

fn extract_metrics_int(doc: &Document, metrics: &mut Vec<u64>) {
    for item in doc {
        let value = item.1;

        extract_metrics_bson_int(value, metrics);
    }
}

pub fn extract_metrics(doc: &Document) -> Vec<u64> {
    let mut metrics: Vec<u64> = Vec::new();
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

pub struct DecodedMetricBlock {
    ref_doc: Rc<Document>,

    pub sample_count: i32,
    pub metrics_count: i32,

    raw_metrics: Vec<u64>,
}

pub fn decode_metric_block<'a>(doc: &'a Document) -> Result<DecodedMetricBlock> {
    let blob = doc.get_binary_generic("data")?;

    let mut size_rdr = Cursor::new(&blob);
    let un_size = size_rdr.read_i32::<LittleEndian>()?;
    // println!("Uncompressed size {}", un_size);

    // skip the length in the compressed blob
    let mut decoded_data = Vec::<u8>::new();
    let mut decoder = Decoder::new(&blob[4..])?;
    decoder.read_to_end(&mut decoded_data)?;

    let mut cur = Cursor::new(&decoded_data);
    let ref_doc = Rc::new(bson::from_reader(&mut cur)?);

    // let mut pos1: usize = cur.position() as usize;
    // println!("pos:{:?}", pos1);

    let metrics_count = cur.read_i32::<LittleEndian>()?;
    // println!("metric_count {}", metrics_count);

    let sample_count = cur.read_i32::<LittleEndian>()?;
    // println!("sample_count {}", sample_count);

    // Extract metrics from reference document
    let ref_metrics = extract_metrics(&ref_doc);
    assert_eq!(ref_metrics.len(), metrics_count as usize);
    // println!("{:?}", ref_metrics);

    let mut scratch = Vec::<u64>::new();
    scratch.resize(ref_metrics.len(), 0);

    // println!("Ref: Sample {} Metric {}", self.sample_count, self.metrics_count);

    // Decode metrics
    let mut raw_metrics = Vec::<u64>::new();

    raw_metrics.reserve((metrics_count * sample_count) as usize);

    let mut zeros_count = 0;

    let mut pos: usize = cur.position() as usize;
    // println!("pos:{:?}", pos);
    let buf = decoded_data.as_ref();

    if sample_count == 0 || metrics_count == 0 {
        return Ok(DecodedMetricBlock {
            ref_doc: ref_doc,
            sample_count: sample_count,
            metrics_count: metrics_count,
            raw_metrics: raw_metrics,
        });
    }

    raw_metrics.resize((sample_count * metrics_count) as usize, 0);

    for i in 0..metrics_count {
        for j in 0..sample_count {
            if zeros_count > 0 {
                raw_metrics[get_array_offset(sample_count, j, i)] = 0;
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

            raw_metrics[get_array_offset(sample_count, j, i)] = val;
        }
    }

    assert_eq!(pos, buf.len());

    // Inflate the metrics
    for i in 0..metrics_count {
        let (v, _) = raw_metrics[get_array_offset(sample_count, 0, i)]
            .overflowing_add(ref_metrics[i as usize] as u64);
        raw_metrics[get_array_offset(sample_count, 0, i)] = v;
    }

    for i in 0..metrics_count {
        for j in 1..sample_count {
            let (v, _) = raw_metrics[get_array_offset(sample_count, j, i)]
                .overflowing_add(raw_metrics[get_array_offset(sample_count, j - 1, i)]);
            raw_metrics[get_array_offset(sample_count, j, i)] = v;
        }
    }

    Ok(DecodedMetricBlock {
        ref_doc: ref_doc,
        sample_count: sample_count,
        metrics_count: metrics_count,
        raw_metrics: raw_metrics,
    })
}

pub struct MetricsReader<'a> {
    doc: &'a Document,
    pub decoded_block: DecodedMetricBlock,

    it_state: MetricState,
    sample: i32,
    scratch: Vec<u64>,
}

impl<'a> MetricsReader<'a> {
    pub fn new<'b>(doc: &'b Document) -> Result<MetricsReader<'b>> {
        let db = decode_metric_block(doc)?;
        let mut s = Vec::new();
        s.resize(db.metrics_count as usize, 0);

        Ok(MetricsReader {
            doc: doc,
            decoded_block: db,
            it_state: MetricState::Reference,
            sample: 0,
            scratch: s,
        })
    }
}

/**
 * Compute the offset into an array for given (sample, metric) pair
 */
fn get_array_offset(sample_count: i32, sample: i32, metric: i32) -> usize {
    ((metric * sample_count) + sample) as usize
}

impl<'a> Iterator for MetricsReader<'a> {
    type Item = MetricsDocument;

    fn next(&mut self) -> Option<MetricsDocument> {
        match self.it_state {
            MetricState::Reference => {
                self.it_state = MetricState::Metrics;

                Some(MetricsDocument::Reference(
                    self.decoded_block.ref_doc.clone(),
                ))
            }
            MetricState::Metrics => {
                if self.sample == self.decoded_block.sample_count {
                    return None;
                }

                self.sample += 1;

                for i in 0..self.decoded_block.metrics_count {
                    self.scratch[i as usize] = self.decoded_block.raw_metrics
                        [get_array_offset(self.decoded_block.sample_count, self.sample - 1, i)];
                }

                let d = fill_document(&self.decoded_block.ref_doc, &&self.scratch);
                Some(MetricsDocument::Metrics(d))
            }
        }
    }
}
