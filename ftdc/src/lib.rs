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
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::ops::Add;

use anyhow::Result;
use bson::doc;
use bson::raw;
use bson::spec::BinarySubtype;
use bson::Bson;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use libflate::zlib::{Decoder, Encoder};
use std::io::Cursor;
use std::rc::Rc;

pub struct BSONMetricsCompressor {
    // samples: usize,
    max_samples: usize,

    metrics: usize,

    // TODO - change this to deltas array so we keep this column oriented in memory
    // also store the un delta encoded metrics as we accumulate samples
    metric_vec: Vec<Vec<u64>>,
    ref_doc: Document,
    ref_doc_vec: Vec<u64>,

    // blocks : Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum AddResult {
    NewBlock (Option<Vec<u8>>),
    ExistingBlock,
}

// TODO - rename this as metric block compressor
// Create FTDC file writer
impl BSONMetricsCompressor {
    pub fn new(max_samples: usize) -> BSONMetricsCompressor {
        BSONMetricsCompressor {
            // samples : 0,
            max_samples,
            metrics: 0,
            metric_vec: Vec::new(),
            ref_doc: Document::new(),
            ref_doc_vec : Vec::new(),
            // blocks: Vec::new()
        }
    }

    // TODO - report if new block was started
    pub fn add_doc(&mut self, doc: &Document) -> Result<AddResult> {
        let mut met_vec = Vec::new();
        extract_metrics_int(doc, &mut met_vec);

        // first document
        if self.ref_doc.is_empty() {
            self.ref_doc = doc.clone();
            
            self.metric_vec.clear();
            self.metrics = met_vec.len();

            self.ref_doc_vec = met_vec;
            // self.samples = 0;

            // self.metric_vec.push(met_vec);
            return Ok(AddResult::NewBlock(None));
        }

        // If metric count the same?
        if self.metrics == met_vec.len() && self.metric_vec.len() < self.max_samples - 1{
            self.metric_vec.push(met_vec);

            return Ok(AddResult::ExistingBlock);

        } else {
            // New block, flush chunk
            let block = self.flush_block()?;

            self.ref_doc = doc.clone();

            self.metric_vec.clear();
            self.metrics = met_vec.len();

            return Ok(AddResult::NewBlock(Some(block)));
        }
    }

    // Compress Metric Vectors
    fn compress_metric_vec(&mut self) -> Vec<u8> {
        assert!(self.metric_vec.len() > 0);

        eprintln!("rdv: {:?}", self.ref_doc_vec);
        eprintln!("ccc: {:?}", self.metric_vec);

        // Do delta calculations
        let metric_count = self.ref_doc_vec.len();
        let sample_count = self.metric_vec.len();

        for s in (1..sample_count).rev() {
            for m in 0..metric_count {
                self.metric_vec[s][m] = self.metric_vec[s][m] - self.metric_vec[s - 1][m]; 
            }
        }
        eprintln!("ccc: {:?}", self.metric_vec);

        for m in 0..metric_count {
            self.metric_vec[0][m] = self.metric_vec[0][m] - self.ref_doc_vec[m]; 
        }


        eprintln!("ccc: {:?}", self.metric_vec);

        // Do RLE
        let mut count_zeros = 0;
        let mut out = Vec::<u8>::new();
        out.resize(metric_count * sample_count * 8, 0);

        let mut offset = 0;

        for m in 0..metric_count {
            for s in 0..sample_count {
                let v = self.metric_vec[s][m];

                if v != 0 {
                    if count_zeros > 0{
                        let write_size = varinteger::encode_with_offset(0, &mut out, offset);
                        offset += write_size;

                        let write_size = varinteger::encode_with_offset(count_zeros - 1, &mut out, offset);
                        offset += write_size;

                        count_zeros = 0;
                        continue;
                    }

                    let write_size = varinteger::encode_with_offset(v, &mut out, offset);
                    offset += write_size;

                } else {
                    count_zeros+=1;
                }
            }
        }

        if count_zeros > 0{
        eprintln!("cz: {:?}", count_zeros);

            let write_size = varinteger::encode_with_offset(0, &mut out, offset);
            offset += write_size;

            let write_size = varinteger::encode_with_offset(count_zeros - 1, &mut out, offset);
            offset += write_size;
        }


        out.resize(offset, 0);

        out
    }

    ///
    /// Format
    /// i32 littlendian
    /// zlib_block
    ///
    /// zlib_block
    /// bson doc
    /// i32 metric
    /// i32 sample
    /// bytes block
    pub fn flush_block(&mut self) -> Result<Vec<u8>> {
        let ref_vec = bson::to_vec(&self.ref_doc)?;

        let mut uncompressed_block: Vec<u8> = Vec::new();
        uncompressed_block.reserve(4 + ref_vec.len()  + self.metrics * self.metric_vec.len());

        uncompressed_block.write_all(&ref_vec)?;

        uncompressed_block.write_i32::<LittleEndian>(self.metrics as i32)?;
        uncompressed_block.write_i32::<LittleEndian>(self.metric_vec.len() as i32)?;

        // Delta & RLE encode
        let encoded_block = self.compress_metric_vec();
        uncompressed_block.write_all(&encoded_block)?;

        // Compress
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&uncompressed_block)?;
        let encoded_data = encoder.finish().into_result()?;

        // Make final block
        let mut final_block = Vec::<u8>::new();
        final_block.reserve(4 + encoded_data.len());
        final_block.write_i32::<LittleEndian>(uncompressed_block.len() as i32)?;
        final_block.write_all(&encoded_data)?;

        Ok(final_block)
    }
}

pub struct BSONBlockWriter < W : Write > {
    writer : BufWriter<W>,
    compressor: BSONMetricsCompressor,
}

impl BSONBlockWriter < File> {
    pub fn new_file(file_name: &str, max_samples: usize) -> Result<BSONBlockWriter<File>> {
        let ff = File::open(file_name)?;

        Ok(BSONBlockWriter {
            writer: BufWriter::new(ff),
            compressor : BSONMetricsCompressor::new(max_samples)
        })
    }
}

impl BSONBlockWriter < bytes::buf::Writer< Vec<u8> > > {
    pub fn new_bytes(buf_mut: &mut bytes::buf::Writer< Vec<u8> >, max_samples: usize) 
        -> Result<BSONBlockWriter< &mut bytes::buf::Writer< Vec<u8> >>> {
        
        Ok(BSONBlockWriter {
            writer: BufWriter::new(buf_mut),
            compressor : BSONMetricsCompressor::new(max_samples)
        })
    }
}

fn gen_metadata_document(doc: &Document) -> Document {
    doc!{
        "_id" : chrono::Utc::now(),
        "type": 0,
        "doc" : doc
    }
}

fn gen_metrics_document(chunk: &[u8]) -> Document {
    doc!{
        "_id" : chrono::Utc::now(),
        "type": 1,
        "doc" : bson::binary::Binary{ subtype: BinarySubtype::Generic, bytes: chunk.to_vec() }
    }
}

fn write_doc_to_writer(writer: &mut dyn Write, doc: &Document) -> Result<()> {
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)?;

    writer.write_all(&buf)?;
    Ok(())
}

// TODO - reduce copies by using raw bson api?
impl<W:Write> BSONBlockWriter< W> {
    pub fn add_metdata_doc(&mut self, doc: &Document) -> Result<()> {
        let md_doc = gen_metadata_document(doc);

        write_doc_to_writer(&mut self.writer, &md_doc)
    }

    pub fn add_sample(&mut self, doc: &Document ) -> Result<()> {
        let result = self.compressor.add_doc(doc)?;
        
        match result {
            AddResult::ExistingBlock => {
                // Do Nothing
            }
            AddResult::NewBlock(block_opt) => {
                if let Some(block) = block_opt {

                    let metric_doc = gen_metrics_document(&block);

                    write_doc_to_writer(&mut self.writer, &metric_doc)?
                }
            }
        }

        Ok(())
    }
}

pub struct BSONBlockReader < R:Read> {
    reader: BufReader<R>,
}

pub enum RawBSONBlock {
    Metadata(Document),
    Metrics(Document),
}

impl BSONBlockReader<File> {
    pub fn new(file_name: &str) -> Result<BSONBlockReader<File>> {
        let ff = File::open(file_name)?;

        Ok(BSONBlockReader {
            reader: BufReader::new(ff),
        })
    }
}

impl<R:Read> BSONBlockReader< R> {
    pub fn new_reader(reader: R) -> Result<BSONBlockReader<R>> {

        Ok(BSONBlockReader {
            reader: BufReader::<R>::new (reader),
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

impl<R:Read> Iterator for BSONBlockReader<R> {
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
        } else if ftdc_type == 2 { // TODO - fix
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
        // eprintln!("ee:{:?}",item);
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

pub enum MetricType {
    Double,
    Int64,
    Int32,
    Boolean,
    DateTime,
    Timestamp,
}

pub struct MetricTypeInfo {
    pub name: String,
    pub metric_type: MetricType,
}

fn extract_metrics_paths_bson_int(
    value: &(&String, &Bson),
    prefix: &str,
    metrics: &mut Vec<MetricTypeInfo>,
) {
    let prefix_dot_str = prefix.to_string() + ".";
    let prefix_dot = prefix_dot_str.as_str();
    let name = &value.0;
    match value.1 {
        &Bson::Double(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo{ name: a1, metric_type: MetricType::Double});
        }
        &Bson::Int64(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo{ name: a1, metric_type: MetricType::Int64});
        }
        &Bson::Int32(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo{ name: a1, metric_type: MetricType::Int32});
        }
        &Bson::Boolean(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo{ name: a1, metric_type: MetricType::Boolean});
        }
        &Bson::DateTime(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo{ name: a1, metric_type: MetricType::DateTime});
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Timestamp(_) => {
            metrics.push(MetricTypeInfo{ name: concat3(prefix_dot, name.as_str(), "t"), metric_type: MetricType::Timestamp});
            metrics.push(MetricTypeInfo{ name: concat3(prefix_dot, name.as_str(), "i"), metric_type: MetricType::Timestamp});
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

fn extract_metrics_paths_int(doc: &Document, prefix: &str, metrics: &mut Vec<MetricTypeInfo>) {
    for item in doc {
        extract_metrics_paths_bson_int(&item, prefix, metrics);
    }
}

pub fn extract_metrics_paths(doc: &Document) -> Vec<MetricTypeInfo> {
    let mut metrics: Vec<MetricTypeInfo> = Vec::new();
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

// TODO - use lifetime to avoid copy of vec
#[derive(Debug)]
pub enum VectorMetricsDocument {
    Reference(Rc<Document>),
    Metrics(Vec<u64>),
}

pub struct DecodedMetricBlock {
    pub ref_doc: Rc<Document>,
    pub ref_doc_size_bytes: usize,

    pub chunk_size_bytes: usize,
    pub sample_count: i32,
    pub metrics_count: i32,

    raw_metrics: Vec<u64>,
}

pub fn decode_metric_block<'a>(doc: &'a Document) -> Result<DecodedMetricBlock> {
    let blob = doc.get_binary_generic("data")?;
    let chunk_size_bytes = blob.len();

    let mut size_rdr = Cursor::new(&blob);
    let un_size = size_rdr.read_i32::<LittleEndian>()?;
    // println!("Uncompressed size {}", un_size);

    // skip the length in the compressed blob
    let mut decoded_data = Vec::<u8>::new();
    let mut decoder = Decoder::new(&blob[4..])?;
    decoder.read_to_end(&mut decoded_data)?;

    let mut cur = Cursor::new(&decoded_data);

    let ref_doc_size_bytes = cur.read_i32::<LittleEndian>()? as usize;
    cur.set_position(0);

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
            ref_doc_size_bytes,
            chunk_size_bytes,
            sample_count: sample_count,
            metrics_count: metrics_count,
            raw_metrics: raw_metrics,
        });
    }

    raw_metrics.resize((sample_count * metrics_count) as usize, 0);

    for i in 0..metrics_count {
        for j in 0..sample_count {
            // eprintln!("r{},{}", i, j);
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
                let read_size = varinteger::decode_with_offset(buf, pos, &mut zeros_count);
                pos += read_size;
            }

            raw_metrics[get_array_offset(sample_count, j, i)] = val;
        }
    }

    assert_eq!(pos, buf.len());

    // eprintln!("ddd: {:?}", raw_metrics);

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
        ref_doc_size_bytes,
        chunk_size_bytes,
        sample_count: sample_count,
        metrics_count: metrics_count,
        raw_metrics: raw_metrics,
    })
}

// TODO - make this a wrapper around VectorMetricsReader
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


pub struct VectorMetricsReader<'a> {
    doc: &'a Document,
    pub decoded_block: DecodedMetricBlock,

    it_state: MetricState,
    sample: i32,
    scratch: Vec<u64>,
}

impl<'a> VectorMetricsReader<'a> {
    pub fn new<'b>(doc: &'b Document) -> Result<VectorMetricsReader<'b>> {
        let db = decode_metric_block(doc)?;
        let mut s = Vec::new();
        s.resize(db.metrics_count as usize, 0);

        Ok(VectorMetricsReader {
            doc: doc,
            decoded_block: db,
            it_state: MetricState::Reference,
            sample: 0,
            scratch: s,
        })
    }

    pub fn get_metrics_count(&self) -> usize {
        self.scratch.len()
    }
}

impl<'a> Iterator for VectorMetricsReader<'a> {
    type Item = VectorMetricsDocument;

    fn next(&mut self) -> Option<VectorMetricsDocument> {
        match self.it_state {
            MetricState::Reference => {
                self.it_state = MetricState::Metrics;

                Some(VectorMetricsDocument::Reference(
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

                Some(VectorMetricsDocument::Metrics(self.scratch.clone()))
            }
        }
    }
}


// struct CompressorTee {
//     docs: Vec<Document>,
    
// }

// impl CompressorTee {
//     fn new() -> CompressorTee {
//         CompressorTee {
//             docs : vec![]
//         }
//     }

//     fn add_doc(&mut self, doc: &Document) {

//     }

//     fn validate(&mut self) {

//     }
// }

// extern crate assert_ok;
use assert_ok::assert_ok;

#[test]
fn test_roundtrip_compressor() {
    let mut writer = BSONMetricsCompressor::new(3);

    assert_eq!( writer.add_doc(&doc! {"a": 1, "x" : 2, "s" : "t"}).unwrap(), AddResult::NewBlock(None));
    assert_eq!( writer.add_doc(&doc! {"a": 2, "x" : 2, "s" : "t"}).unwrap(), AddResult::ExistingBlock);
    assert_eq!( writer.add_doc(&doc! {"a": 3, "x" : 2, "s" : "t"}).unwrap(), AddResult::ExistingBlock);

    let addresult = writer.add_doc(&doc! {"a": 7, "x" : 9, "s" : "t"}).unwrap();

    assert_ne!( addresult, AddResult::ExistingBlock);

    match addresult {
        AddResult::ExistingBlock => {
            assert!(false);
        }
        AddResult::NewBlock(met_opt)=> {
            let met = met_opt.unwrap();

            /* 
            let mut rdr = BSONBlockReader::new_reader(Cursor::new(met)).unwrap();

            let bb = rdr.next();
            assert!(bb.is_some());
            */

            let d1 = doc!{ "data" : bson::Binary{subtype: BinarySubtype::Generic, bytes: met} };
            let mut dmbr = decode_metric_block(&d1);
            assert!(dmbr.is_ok());
            let dmb = dmbr.unwrap();
            assert_eq!(dmb.sample_count, 2);
            assert_eq!(dmb.metrics_count, 2);
            eprintln!("{:?}", dmb.ref_doc);
            assert_eq!(dmb.raw_metrics, vec![2, 3, 2, 2]);
        }
    }

}


#[test]
fn test_roundtrip_bson() {
    let mut buf = Vec::with_capacity(1024).writer();

    let mut writer = BSONBlockWriter::new_bytes(&mut buf, 3)?;

    assert_ok!( writer.add_sample(&doc! {"a": 1, "x" : 2, "s" : "t"}) );
    assert_ok!( writer.add_sample(&doc! {"a": 2, "x" : 2, "s" : "t"}) );
    assert_ok!( writer.add_sample(&doc! {"a": 3, "x" : 2, "s" : "t"}) );

    //et addresult = writer.add_doc(&doc! {"a": 7, "x" : 9, "s" : "t"}).unwrap();


}

